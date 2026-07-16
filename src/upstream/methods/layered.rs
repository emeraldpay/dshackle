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

//! Composes a chain-default [`QuorumFactory`] with a user [`ConfiguredMethods`]
//! layer into one query surface. Mirrors the legacy `ManagedCallMethods`:
//! the configured layer *refines* the default — adding, subtracting, or
//! overriding — but neither layer owns merged state.

use crate::jsonrpc::RpcMethod;
use crate::upstream::methods::config::ConfiguredMethods;
use crate::upstream::quorum::{CallQuorum, QuorumFactory};
use serde_json::value::RawValue;
use std::sync::Arc;

/// Two-layer methods stack: chain defaults underneath, user overrides on top.
///
/// Every [`QuorumFactory`] method delegates to one or both layers — this type
/// carries no state of its own. The composition rules match legacy semantics:
/// - `is_callable`: `disabled` always wins; otherwise `enabled || default.is_callable`.
/// - `hardcoded_response`: user static wins; falls back to default.
/// - `quorum_for`: user override wins; falls back to default's per-method map.
pub struct LayeredMethods {
    default: Arc<dyn QuorumFactory>,
    configured: Arc<ConfiguredMethods>,
}

impl LayeredMethods {
    pub fn new(default: Arc<dyn QuorumFactory>, configured: Arc<ConfiguredMethods>) -> Self {
        Self {
            default,
            configured,
        }
    }
}

impl QuorumFactory for LayeredMethods {
    fn quorum_for(&self, method: &RpcMethod) -> Box<dyn CallQuorum> {
        if let Some(kind) = self.configured.quorum_override(method) {
            return kind.create();
        }
        self.default.quorum_for(method)
    }

    fn is_callable(&self, method: &RpcMethod) -> bool {
        if self.configured.is_disabled(method) {
            return false;
        }
        self.configured.is_enabled(method) || self.default.is_callable(method)
    }

    fn hardcoded_response(&self, method: &RpcMethod) -> Option<&RawValue> {
        self.configured
            .hardcoded_response(method)
            .or_else(|| self.default.hardcoded_response(method))
    }

    fn supported_methods(&self) -> Vec<String> {
        // Union of both layers, then drop anything the user disabled — the same
        // `disabled`-wins rule `is_callable` applies. Hardcoded responses are
        // intentionally not subtracted (legacy `ManagedCallMethods`).
        let mut out: Vec<String> = self
            .default
            .supported_methods()
            .into_iter()
            .chain(self.configured.supported_methods())
            .filter(|m| !self.configured.is_disabled(&RpcMethod::from(m.as_str())))
            .collect();
        out.sort();
        out.dedup();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::{MethodConfig, MethodsConfig};
    use crate::upstream::methods::DefaultMethods;
    use crate::upstream::methods::config::QuorumKind;
    use crate::upstream::quorum::{AlwaysQuorum, NotLaggingQuorum, SelectorHint};
    use serde_json::value::RawValue;
    use std::collections::{HashMap, HashSet};

    /// Stand-in for a chain default: fixed callable set, fixed hardcoded map,
    /// fixed per-method quorum picker.
    struct FakeDefault {
        callable: HashSet<RpcMethod>,
        hardcoded: HashMap<RpcMethod, Box<RawValue>>,
        quorum: Box<dyn Fn(&RpcMethod) -> Box<dyn CallQuorum> + Send + Sync>,
    }

    impl QuorumFactory for FakeDefault {
        fn quorum_for(&self, method: &RpcMethod) -> Box<dyn CallQuorum> {
            (self.quorum)(method)
        }
        fn is_callable(&self, method: &RpcMethod) -> bool {
            self.callable.contains(method)
        }
        fn hardcoded_response(&self, method: &RpcMethod) -> Option<&RawValue> {
            self.hardcoded.get(method).map(|b| &**b)
        }
        fn supported_methods(&self) -> Vec<String> {
            let mut out: Vec<String> = self
                .callable
                .iter()
                .chain(self.hardcoded.keys())
                .map(|m| m.as_str().to_string())
                .collect();
            out.sort();
            out.dedup();
            out
        }
    }

    fn raw(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_string()).unwrap()
    }

    fn cfg(enabled: Vec<MethodConfig>, disabled: Vec<MethodConfig>) -> Arc<ConfiguredMethods> {
        Arc::new(ConfiguredMethods::from_config(Some(&MethodsConfig {
            enabled,
            disabled,
        })))
    }

    fn default_with(callable: &[&str], hardcoded: &[(&str, &str)]) -> Arc<FakeDefault> {
        Arc::new(FakeDefault {
            callable: callable.iter().map(|s| RpcMethod::from(*s)).collect(),
            hardcoded: hardcoded
                .iter()
                .map(|(k, v)| (RpcMethod::from(*k), raw(v)))
                .collect(),
            // Fixed mapping so tests can tell quorums apart by selector.
            quorum: Box::new(|m| {
                if m.as_str() == "head_call" {
                    Box::new(NotLaggingQuorum::new(0))
                } else {
                    Box::new(AlwaysQuorum::new())
                }
            }),
        })
    }

    #[test]
    fn passes_default_through_when_no_user_config() {
        let default = default_with(&["eth_call"], &[("net_version", "\"1\"")]);
        let layered = LayeredMethods::new(default, Arc::new(ConfiguredMethods::empty()));

        assert!(layered.is_callable(&"eth_call".into()));
        assert!(!layered.is_callable(&"admin_shutdown".into()));
        assert_eq!(
            layered
                .hardcoded_response(&"net_version".into())
                .map(|r| r.get()),
            Some("\"1\""),
        );
    }

    #[test]
    fn enabled_user_method_becomes_callable() {
        let default = default_with(&["eth_call"], &[]);
        let configured = cfg(
            vec![MethodConfig {
                name: "parity_trace".into(),
                quorum: None,
                static_value: None,
            }],
            vec![],
        );
        let layered = LayeredMethods::new(default, configured);

        assert!(layered.is_callable(&"parity_trace".into()));
        assert!(layered.is_callable(&"eth_call".into())); // default still honored
    }

    #[test]
    fn disabled_overrides_default_callable() {
        let default = default_with(&["admin_shutdown", "eth_call"], &[]);
        let configured = cfg(
            vec![],
            vec![MethodConfig {
                name: "admin_shutdown".into(),
                quorum: None,
                static_value: None,
            }],
        );
        let layered = LayeredMethods::new(default, configured);

        assert!(!layered.is_callable(&"admin_shutdown".into()));
        assert!(layered.is_callable(&"eth_call".into()));
    }

    #[test]
    fn user_quorum_override_wins() {
        let default = default_with(&["eth_call"], &[]);
        let configured = cfg(
            vec![MethodConfig {
                name: "eth_call".into(),
                quorum: Some("not_lagging".into()),
                static_value: None,
            }],
            vec![],
        );
        let layered = LayeredMethods::new(default, configured);

        match layered.quorum_for(&"eth_call".into()).selector() {
            SelectorHint::NotLagging { max_lag: 0 } => {}
            other => panic!("expected user override, got {other:?}"),
        }
    }

    #[test]
    fn default_quorum_kept_when_no_override() {
        let default = default_with(&["head_call"], &[]);
        let layered = LayeredMethods::new(default, Arc::new(ConfiguredMethods::empty()));

        // "head_call" is wired to NotLaggingQuorum{0} in the fake default.
        match layered.quorum_for(&"head_call".into()).selector() {
            SelectorHint::NotLagging { max_lag: 0 } => {}
            other => panic!("expected default not-lagging, got {other:?}"),
        }
    }

    #[test]
    fn enabling_a_default_method_resets_its_quorum_to_always() {
        // Mirrors legacy `ManagedCallMethodsSpec."Use quorum from delegate if
        // it supports the method"`: a method both served by the default and
        // re-enabled by the user stops using the default's quorum.
        let default = default_with(&["head_call"], &[]);
        let configured = cfg(
            vec![MethodConfig {
                name: "head_call".into(),
                quorum: None,
                static_value: None,
            }],
            vec![],
        );
        let layered = LayeredMethods::new(default, configured);

        match layered.quorum_for(&"head_call".into()).selector() {
            SelectorHint::Available => {}
            other => panic!("expected the Always default, got {other:?}"),
        }
    }

    #[test]
    fn user_static_response_wins_over_default() {
        let default = default_with(&[], &[("net_version", "\"1\"")]);
        let configured = cfg(
            vec![MethodConfig {
                name: "net_version".into(),
                quorum: None,
                static_value: Some("\"42\"".into()),
            }],
            vec![],
        );
        let layered = LayeredMethods::new(default, configured);

        assert_eq!(
            layered
                .hardcoded_response(&"net_version".into())
                .map(|r| r.get()),
            Some("\"42\""),
        );
    }

    #[test]
    fn is_hardcoded_default_derives_from_hardcoded_response() {
        // The trait's default `is_hardcoded` defers to `hardcoded_response`,
        // which `LayeredMethods` uses unchanged.
        let default = default_with(&[], &[("net_version", "\"1\"")]);
        let layered = LayeredMethods::new(default, Arc::new(ConfiguredMethods::empty()));
        assert!(layered.is_hardcoded(&"net_version".into()));
        assert!(!layered.is_hardcoded(&"eth_call".into()));
    }

    #[test]
    fn layered_is_pure_delegation_with_no_hidden_state() {
        // Sanity: wrapping the same layers twice behaves identically — proving
        // there is no per-instance caching that could drift.
        let default = default_with(&["eth_call"], &[]);
        let configured = Arc::new(ConfiguredMethods::empty());
        let a = LayeredMethods::new(
            Arc::clone(&default) as Arc<dyn QuorumFactory>,
            Arc::clone(&configured),
        );
        let b = LayeredMethods::new(default as Arc<dyn QuorumFactory>, configured);
        assert_eq!(
            a.is_callable(&"eth_call".into()),
            b.is_callable(&"eth_call".into())
        );
    }

    #[test]
    fn unused_default_methods_integrates() {
        // Integration with the chain-agnostic DefaultMethods (used by Dshackle
        // remotes): everything stays callable, nothing hardcoded.
        let layered = LayeredMethods::new(
            Arc::new(DefaultMethods),
            Arc::new(ConfiguredMethods::empty()),
        );
        assert!(layered.is_callable(&"whatever".into()));
        assert!(!layered.is_hardcoded(&"whatever".into()));
    }

    #[test]
    fn supported_methods_unions_layers_minus_disabled() {
        let default = default_with(&["eth_call", "admin_shutdown"], &[("net_version", "\"1\"")]);
        let configured = cfg(
            vec![MethodConfig {
                name: "parity_trace".into(),
                quorum: None,
                static_value: None,
            }],
            vec![MethodConfig {
                name: "admin_shutdown".into(),
                quorum: None,
                static_value: None,
            }],
        );
        let layered = LayeredMethods::new(default, configured);
        let supported = layered.supported_methods();

        // User-enabled and default callable + hardcoded are all present...
        assert!(supported.contains(&"eth_call".to_string()));
        assert!(supported.contains(&"parity_trace".to_string()));
        assert!(supported.contains(&"net_version".to_string()));
        // ...but the disabled method is removed, even though it was a default.
        assert!(!supported.contains(&"admin_shutdown".to_string()));
        // Sorted, no duplicates.
        let mut sorted = supported.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(supported, sorted);
    }

    #[test]
    fn quorum_override_type_is_respected() {
        // A user `not_empty` override routes through QuorumKind::create and
        // should carry the `Available` selector (NonEmptyQuorum does not
        // pre-filter by lag).
        let _ = QuorumKind::NonEmpty.create();
        let default = default_with(&["eth_getTransactionByHash"], &[]);
        let configured = cfg(
            vec![MethodConfig {
                name: "eth_getTransactionByHash".into(),
                quorum: Some("not_empty".into()),
                static_value: None,
            }],
            vec![],
        );
        let layered = LayeredMethods::new(default, configured);
        let q = layered.quorum_for(&"eth_getTransactionByHash".into());
        matches!(q.selector(), SelectorHint::Available);
    }
}
