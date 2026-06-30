// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The "configured" methods layer: everything the user asked for in YAML and
//! nothing from the chain defaults. Designed to be composed with a default
//! layer by [`LayeredMethods`](super::LayeredMethods).

use crate::config::upstreams::MethodsConfig;
use crate::jsonrpc::RpcMethod;
use crate::upstream::quorum::{
    AlwaysQuorum, CallQuorum, NonEmptyQuorum, NotLaggingQuorum, QuorumFactory,
};
use serde_json::value::RawValue;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

/// Quorum strategies that can be selected from the YAML config. Mirrors the
/// legacy `ManagedCallMethods.setQuorum` id parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumKind {
    Always,
    NotLagging,
    NonEmpty,
}

impl QuorumKind {
    /// Instantiate a fresh quorum of this kind. Each call produces a new
    /// stateful quorum — never share instances between requests.
    pub fn create(self) -> Box<dyn CallQuorum> {
        match self {
            QuorumKind::Always => Box::new(AlwaysQuorum::new()),
            QuorumKind::NotLagging => Box::new(NotLaggingQuorum::new(0)),
            QuorumKind::NonEmpty => Box::new(NonEmptyQuorum::new()),
        }
    }
}

impl FromStr for QuorumKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept the many aliases used in the legacy config (snake/kebab,
        // "not-empty"/"non-empty", etc.) so existing YAML files keep working.
        match s.trim().to_ascii_lowercase().as_str() {
            "always" => Ok(QuorumKind::Always),
            "no-lag" | "not-lagging" | "no_lag" | "not_lagging" => Ok(QuorumKind::NotLagging),
            "not-empty" | "not_empty" | "non-empty" | "non_empty" => Ok(QuorumKind::NonEmpty),
            _ => Err(()),
        }
    }
}

/// Holds exactly what the user put under `methods:` for one upstream — no
/// chain defaults, no aggregation. Intended to be combined with a default
/// layer via [`LayeredMethods`](super::LayeredMethods::new).
///
/// Semantics of each field:
/// - `enabled`: methods added to the callable set (and possibly paired with
///   a quorum override or a static response).
/// - `disabled`: methods stripped from the callable set; does not affect
///   hardcoded responses (matches legacy `ManagedCallMethods`).
/// - `quorum_overrides`: per-method quorum choice that outranks the default.
/// - `static_responses`: per-method hardcoded response that bypasses the node.
pub struct ConfiguredMethods {
    enabled: HashSet<RpcMethod>,
    disabled: HashSet<RpcMethod>,
    quorum_overrides: HashMap<RpcMethod, QuorumKind>,
    static_responses: HashMap<RpcMethod, Box<RawValue>>,
}

impl ConfiguredMethods {
    /// Empty configured layer — equivalent to the user providing no
    /// `methods:` block. Composing this with a default layer produces the
    /// default behaviour unchanged.
    pub fn empty() -> Self {
        Self {
            enabled: HashSet::new(),
            disabled: HashSet::new(),
            quorum_overrides: HashMap::new(),
            static_responses: HashMap::new(),
        }
    }

    /// Build from an optional YAML block. An absent block yields [`empty`].
    /// Unknown quorum ids are logged and skipped (legacy behaviour).
    pub fn from_config(config: Option<&MethodsConfig>) -> Self {
        let Some(cfg) = config else {
            return Self::empty();
        };
        let mut out = Self::empty();

        for m in &cfg.enabled {
            let method = RpcMethod::from(m.name.as_str());
            out.enabled.insert(method.clone());

            if let Some(q) = m.quorum.as_deref() {
                match QuorumKind::from_str(q) {
                    Ok(kind) => {
                        out.quorum_overrides.insert(method.clone(), kind);
                    }
                    Err(_) => {
                        tracing::warn!("Unknown quorum '{}' for method {}", q, m.name);
                    }
                }
            }

            if let Some(s) = m.static_value.as_deref() {
                out.static_responses
                    .insert(method, parse_static_response(s));
            }
        }

        for m in &cfg.disabled {
            out.disabled.insert(RpcMethod::from(m.name.as_str()));
        }

        out
    }

    /// Build from a bare allow-list, as produced by a Dshackle remote's
    /// `Describe`. No quorum overrides or static responses — this is just a
    /// callable gate.
    pub fn allowed_only(methods: HashSet<RpcMethod>) -> Self {
        Self {
            enabled: methods,
            disabled: HashSet::new(),
            quorum_overrides: HashMap::new(),
            static_responses: HashMap::new(),
        }
    }

    /// Whether the user explicitly disabled this method.
    pub fn is_disabled(&self, method: &RpcMethod) -> bool {
        self.disabled.contains(method)
    }

    /// Whether the user explicitly enabled this method.
    pub fn is_enabled(&self, method: &RpcMethod) -> bool {
        self.enabled.contains(method)
    }

    /// User-supplied quorum override for this method, if any.
    pub fn quorum_override(&self, method: &RpcMethod) -> Option<QuorumKind> {
        self.quorum_overrides.get(method).copied()
    }
}

impl QuorumFactory for ConfiguredMethods {
    /// Only meaningful when used standalone: returns the user-supplied
    /// quorum or [`AlwaysQuorum`] as a safe fallback. In a layered stack,
    /// [`LayeredMethods`](super::LayeredMethods) consults the override map
    /// directly so the default layer keeps its per-method mapping when no
    /// override is configured.
    fn quorum_for(&self, method: &RpcMethod) -> Box<dyn CallQuorum> {
        self.quorum_override(method)
            .map(QuorumKind::create)
            .unwrap_or_else(|| Box::new(AlwaysQuorum::new()))
    }

    fn is_callable(&self, method: &RpcMethod) -> bool {
        // A method is callable at this layer only if the user added it and
        // did not also disable it. Disabling a user-added method is odd but
        // we treat it consistently with subtractive semantics.
        self.is_enabled(method) && !self.is_disabled(method)
    }

    fn hardcoded_response(&self, method: &RpcMethod) -> Option<&RawValue> {
        self.static_responses.get(method).map(|b| &**b)
    }

    /// The methods this layer adds on its own: those the user enabled plus any
    /// with a static response. The chain default's methods are folded in by
    /// [`LayeredMethods`](super::LayeredMethods), which also applies `disabled`.
    fn supported_methods(&self) -> Vec<String> {
        let mut names: HashSet<String> =
            self.enabled.iter().map(|m| m.as_str().to_string()).collect();
        names.extend(self.static_responses.keys().map(|m| m.as_str().to_string()));
        let mut out: Vec<String> = names.into_iter().collect();
        out.sort();
        out
    }
}

/// Encode a user-supplied static response. If the string parses as JSON we
/// forward it as-is; otherwise we wrap it as a JSON string so it's always a
/// valid JSON-RPC `result`. This matches the legacy behavior in
/// `ManagedCallMethods.executeHardcoded`.
fn parse_static_response(raw: &str) -> Box<RawValue> {
    match RawValue::from_string(raw.to_string()) {
        Ok(v) if serde_json::from_str::<serde_json::Value>(raw).is_ok() => v,
        _ => {
            let encoded = serde_json::to_string(raw).unwrap_or_else(|_| "\"\"".into());
            RawValue::from_string(encoded).expect("serde_json produced invalid JSON")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::MethodConfig;

    #[test]
    fn quorum_kind_aliases() {
        assert_eq!(QuorumKind::from_str("always").unwrap(), QuorumKind::Always);
        assert_eq!(
            QuorumKind::from_str("not_lagging").unwrap(),
            QuorumKind::NotLagging
        );
        assert_eq!(
            QuorumKind::from_str("no-lag").unwrap(),
            QuorumKind::NotLagging
        );
        assert_eq!(
            QuorumKind::from_str("not_empty").unwrap(),
            QuorumKind::NonEmpty
        );
        assert_eq!(
            QuorumKind::from_str("non-empty").unwrap(),
            QuorumKind::NonEmpty
        );
        assert_eq!(
            QuorumKind::from_str("NOT_EMPTY").unwrap(),
            QuorumKind::NonEmpty
        );
        assert!(QuorumKind::from_str("bogus").is_err());
    }

    #[test]
    fn empty_config_claims_nothing() {
        let c = ConfiguredMethods::empty();
        assert!(!c.is_enabled(&"anything".into()));
        assert!(!c.is_disabled(&"anything".into()));
        assert!(!c.is_callable(&"anything".into()));
        assert!(c.hardcoded_response(&"anything".into()).is_none());
    }

    #[test]
    fn enabled_methods_claim_callable() {
        let cfg = MethodsConfig {
            enabled: vec![
                MethodConfig {
                    name: "parity_trace".into(),
                    quorum: None,
                    static_value: None,
                },
                MethodConfig {
                    name: "eth_call".into(),
                    quorum: Some("not_empty".into()),
                    static_value: None,
                },
            ],
            disabled: vec![MethodConfig {
                name: "admin_shutdown".into(),
                quorum: None,
                static_value: None,
            }],
        };
        let c = ConfiguredMethods::from_config(Some(&cfg));

        assert!(c.is_callable(&"parity_trace".into()));
        assert!(c.is_callable(&"eth_call".into()));
        assert!(c.is_disabled(&"admin_shutdown".into()));
        assert_eq!(
            c.quorum_override(&"eth_call".into()),
            Some(QuorumKind::NonEmpty),
        );
    }

    #[test]
    fn disabled_blocks_a_self_enabled_method() {
        // Disabling a method that was also enabled removes callability —
        // disable always wins.
        let cfg = MethodsConfig {
            enabled: vec![MethodConfig {
                name: "trace_it".into(),
                quorum: None,
                static_value: None,
            }],
            disabled: vec![MethodConfig {
                name: "trace_it".into(),
                quorum: None,
                static_value: None,
            }],
        };
        let c = ConfiguredMethods::from_config(Some(&cfg));
        assert!(!c.is_callable(&"trace_it".into()));
    }

    #[test]
    fn unknown_quorum_is_dropped_not_fatal() {
        let cfg = MethodsConfig {
            enabled: vec![MethodConfig {
                name: "debug_x".into(),
                quorum: Some("mystery".into()),
                static_value: None,
            }],
            disabled: vec![],
        };
        let c = ConfiguredMethods::from_config(Some(&cfg));
        assert!(c.is_callable(&"debug_x".into()));
        assert!(c.quorum_override(&"debug_x".into()).is_none());
    }

    #[test]
    fn static_value_json_preserved() {
        let cfg = MethodsConfig {
            enabled: vec![MethodConfig {
                name: "net_version".into(),
                quorum: None,
                static_value: Some("\"42\"".into()),
            }],
            disabled: vec![],
        };
        let c = ConfiguredMethods::from_config(Some(&cfg));
        assert_eq!(
            c.hardcoded_response(&"net_version".into()).map(|r| r.get()),
            Some("\"42\""),
        );
    }

    #[test]
    fn static_value_nonjson_is_encoded_as_string() {
        let cfg = MethodsConfig {
            enabled: vec![MethodConfig {
                name: "web3_clientVersion".into(),
                quorum: None,
                static_value: Some("Geth/v1.2.3".into()),
            }],
            disabled: vec![],
        };
        let c = ConfiguredMethods::from_config(Some(&cfg));
        assert_eq!(
            c.hardcoded_response(&"web3_clientVersion".into())
                .map(|r| r.get()),
            Some("\"Geth/v1.2.3\""),
        );
    }
}
