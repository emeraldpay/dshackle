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

//! Upstream connection configuration: individual upstreams, connection types,
//! options, methods, and default options.

use crate::blockchain::TargetBlockchain;
use crate::config::bytes::deserialize_opt_bytes;
use crate::config::tls::{BasicAuth, ClientTlsAuth};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;
use tracing::warn;

/// Upstream ID validation: must start with a letter, contain letters/digits/hyphens/underscores,
/// and end with a letter or digit. Minimum 3 characters.
static UPSTREAM_ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z][a-zA-Z0-9_-]+[a-zA-Z0-9]$").unwrap());

// ─── Upstream Options ────────────────────────────────────────────────────────

/// Partial options that can be merged (defaults + per-upstream overrides).
/// All fields are optional; `None` means "not configured, use default".
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct PartialOptions {
    #[serde(alias = "disableValidation", rename = "disable-validation")]
    pub disable_validation: Option<bool>,
    #[serde(alias = "validateSyncing", rename = "validate-syncing")]
    pub validate_syncing: Option<bool>,
    #[serde(alias = "validatePeers", rename = "validate-peers")]
    pub validate_peers: Option<bool>,
    #[serde(alias = "minPeers", rename = "min-peers")]
    pub min_peers: Option<i32>,
    #[serde(alias = "validationInterval", rename = "validation-interval")]
    pub validation_interval: Option<i32>,
    pub timeout: Option<i32>,
    pub priority: Option<i32>,
    pub balance: Option<bool>,
}

impl PartialOptions {
    /// Merges another set of options on top of self. Values from `overwrites`
    /// take precedence when set.
    pub fn merge(&self, overwrites: &PartialOptions) -> PartialOptions {
        PartialOptions {
            disable_validation: overwrites.disable_validation.or(self.disable_validation),
            validate_syncing: overwrites.validate_syncing.or(self.validate_syncing),
            validate_peers: overwrites.validate_peers.or(self.validate_peers),
            min_peers: overwrites.min_peers.or(self.min_peers),
            validation_interval: overwrites.validation_interval.or(self.validation_interval),
            timeout: overwrites.timeout.or(self.timeout),
            priority: overwrites.priority.or(self.priority),
            balance: overwrites.balance.or(self.balance),
        }
    }

    /// Resolve into effective [`Options`], applying the legacy defaults for
    /// everything left unset. Out-of-range values are clamped rather than
    /// rejected, because by this point the config has already been accepted.
    pub fn build(&self) -> Options {
        Options {
            disable_validation: self.disable_validation.unwrap_or(false),
            validate_syncing: self.validate_syncing.unwrap_or(true),
            validate_peers: self.validate_peers.unwrap_or(true),
            min_peers: self.min_peers.unwrap_or(1).max(0) as u32,
            validation_interval: Duration::from_secs(
                self.validation_interval.unwrap_or(30).max(1) as u64,
            ),
            timeout: Duration::from_secs(self.timeout.unwrap_or(60).max(1) as u64),
            priority: self.priority.unwrap_or(10).clamp(0, 1_000_000),
            balance: self.balance.unwrap_or(false),
        }
    }
}

/// Effective upstream options with all defaults applied — the resolved form
/// of [`PartialOptions`], matching the legacy `UpstreamsConfig.Options`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// Treat the upstream as always valid: no validation probes run and the
    /// lag-based status is ignored.
    pub disable_validation: bool,
    /// Check `eth_syncing` during validation.
    pub validate_syncing: bool,
    /// Check the peer count against `min_peers` during validation.
    pub validate_peers: bool,
    /// Minimum number of peers for the upstream to be trusted.
    pub min_peers: u32,
    /// How often to run the validation probes.
    pub validation_interval: Duration,
    /// Overall request timeout for the upstream.
    pub timeout: Duration,
    pub priority: i32,
    /// Whether the upstream can be used for balance lookups.
    pub balance: bool,
}

// ─── Default Options ─────────────────────────────────────────────────────────

/// Default options that apply to all upstreams of specified blockchains.
/// Deserialized from the `defaults` / `defaultOptions` YAML list entries.
#[derive(Debug, Clone, Deserialize)]
pub struct DefaultOptions {
    /// Blockchain names these defaults apply to.
    #[serde(alias = "chains")]
    pub blockchains: Option<Vec<String>>,
    /// Options can be nested under an `options` key or specified directly.
    pub options: Option<PartialOptions>,
    // Direct option fields (same level as `blockchains`)
    #[serde(alias = "minPeers", rename = "min-peers")]
    pub min_peers: Option<i32>,
    #[serde(alias = "validatePeers", rename = "validate-peers")]
    pub validate_peers: Option<bool>,
    #[serde(alias = "validateSyncing", rename = "validate-syncing")]
    pub validate_syncing: Option<bool>,
    #[serde(alias = "validationInterval", rename = "validation-interval")]
    pub validation_interval: Option<i32>,
    pub timeout: Option<i32>,
}

impl DefaultOptions {
    /// Returns the merged options from both the nested `options` block
    /// and any directly-specified fields.
    pub fn resolved_options(&self) -> PartialOptions {
        let direct = PartialOptions {
            min_peers: self.min_peers,
            validate_peers: self.validate_peers,
            validate_syncing: self.validate_syncing,
            validation_interval: self.validation_interval,
            timeout: self.timeout,
            ..Default::default()
        };
        match &self.options {
            Some(nested) => nested.merge(&direct),
            None => direct,
        }
    }
}

// ─── Methods ─────────────────────────────────────────────────────────────────

/// RPC methods configuration for an upstream.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct MethodsConfig {
    pub enabled: Vec<MethodConfig>,
    pub disabled: Vec<MethodConfig>,
}

/// A single RPC method configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct MethodConfig {
    pub name: String,
    pub quorum: Option<String>,
    #[serde(rename = "static")]
    pub static_value: Option<String>,
}

// ─── Roles ───────────────────────────────────────────────────────────────────

/// Upstream role in the load balancing hierarchy.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum UpstreamRole {
    #[default]
    Primary,
    Secondary,
    Fallback,
}

impl UpstreamRole {
    pub fn from_str_lenient(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "primary" | "standard" => Some(UpstreamRole::Primary),
            "secondary" => Some(UpstreamRole::Secondary),
            "fallback" => Some(UpstreamRole::Fallback),
            _ => None,
        }
    }
}

// ─── Connection types ────────────────────────────────────────────────────────

/// Ethereum upstream connection (JSON RPC + optional WebSocket).
#[derive(Debug, Clone, Deserialize)]
pub struct EthereumConnection {
    pub rpc: Option<HttpEndpoint>,
    pub ws: Option<WsEndpoint>,
}

/// Bitcoin upstream connection (JSON RPC + optional Esplora + optional ZeroMQ).
#[derive(Debug, Clone, Deserialize)]
pub struct BitcoinConnection {
    pub rpc: Option<HttpEndpoint>,
    pub esplora: Option<HttpEndpoint>,
    #[serde(alias = "zeromq")]
    pub zeromq: Option<ZeromqEndpoint>,
}

/// Dshackle gRPC upstream connection.
#[derive(Debug, Clone, Deserialize)]
pub struct DshackleConnection {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub url: Option<String>,
    pub tls: Option<ClientTlsAuth>,
    pub compress: Option<bool>,
}

/// HTTP JSON RPC endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct HttpEndpoint {
    pub url: String,
    #[serde(alias = "basicAuth", rename = "basic-auth")]
    pub basic_auth: Option<BasicAuth>,
    pub tls: Option<ClientTlsAuth>,
    #[serde(default)]
    pub compress: Option<bool>,
}

/// WebSocket endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WsEndpoint {
    pub url: String,
    pub origin: Option<String>,
    #[serde(alias = "basicAuth", rename = "basic-auth")]
    pub basic_auth: Option<BasicAuth>,
    #[serde(
        alias = "frameSize",
        rename = "frame-size",
        deserialize_with = "deserialize_opt_bytes",
        default
    )]
    pub frame_size: Option<usize>,
    #[serde(
        alias = "msgSize",
        rename = "msg-size",
        deserialize_with = "deserialize_opt_bytes",
        default
    )]
    pub msg_size: Option<usize>,
    pub connections: Option<u32>,
    pub compress: Option<bool>,
    #[serde(
        alias = "disable-methods",
        rename = "disabled-methods",
        default
    )]
    pub disabled_methods: Vec<String>,
}

impl Default for WsEndpoint {
    fn default() -> Self {
        Self {
            url: String::new(),
            origin: None,
            basic_auth: None,
            frame_size: None,
            msg_size: None,
            connections: None,
            compress: None,
            disabled_methods: Vec::new(),
        }
    }
}

/// ZeroMQ endpoint for Bitcoin block notifications.
#[derive(Debug, Clone, Deserialize)]
pub struct ZeromqEndpoint {
    #[serde(alias = "address")]
    pub url: String,
}

// ─── Raw connection wrapper (determines upstream type) ───────────────────────

/// Raw YAML connection block — exactly one of the connection types should be set.
#[derive(Debug, Clone, Deserialize)]
pub struct RawConnection {
    pub ethereum: Option<EthereumConnection>,
    pub bitcoin: Option<BitcoinConnection>,
    #[serde(alias = "grpc")]
    pub dshackle: Option<DshackleConnection>,
}

/// Resolved upstream connection type.
#[derive(Debug, Clone)]
pub enum UpstreamConnection {
    Ethereum(EthereumConnection),
    Bitcoin(BitcoinConnection),
    Dshackle(DshackleConnection),
}

// ─── Upstream entry ──────────────────────────────────────────────────────────

/// Raw upstream entry as it appears in YAML (before processing).
#[derive(Debug, Clone, Deserialize)]
pub struct RawUpstream {
    pub id: Option<String>,
    #[serde(alias = "chain")]
    pub blockchain: Option<String>,
    pub enabled: Option<bool>,
    pub role: Option<String>,
    #[serde(default, deserialize_with = "deserialize_labels")]
    pub labels: HashMap<String, String>,
    pub methods: Option<MethodsConfig>,
    pub connection: Option<RawConnection>,

    // Options can appear directly on the upstream entry
    #[serde(alias = "disableValidation", rename = "disable-validation")]
    pub disable_validation: Option<bool>,
    #[serde(alias = "validateSyncing", rename = "validate-syncing")]
    pub validate_syncing: Option<bool>,
    #[serde(alias = "validatePeers", rename = "validate-peers")]
    pub validate_peers: Option<bool>,
    #[serde(alias = "minPeers", rename = "min-peers")]
    pub min_peers: Option<i32>,
    #[serde(alias = "validationInterval", rename = "validation-interval")]
    pub validation_interval: Option<i32>,
    pub timeout: Option<i32>,
    pub priority: Option<i32>,
    pub balance: Option<bool>,

    /// Options nested under the `options` key (backward compatibility).
    pub options: Option<PartialOptions>,
}

/// Processed upstream with resolved connection and options.
#[derive(Debug, Clone)]
pub struct Upstream {
    pub id: String,
    pub blockchain: Option<String>,
    pub enabled: bool,
    pub role: UpstreamRole,
    pub labels: HashMap<String, String>,
    pub methods: Option<MethodsConfig>,
    pub connection: UpstreamConnection,
    pub options: PartialOptions,
}

// ─── Upstreams config (cluster section) ──────────────────────────────────────

/// Include paths can be a single string or a list.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum IncludeValue {
    Single(String),
    List(Vec<String>),
}

impl From<IncludeValue> for Vec<String> {
    fn from(value: IncludeValue) -> Self {
        match value {
            IncludeValue::Single(s) => vec![s],
            IncludeValue::List(v) => v,
        }
    }
}

/// Raw upstreams config as deserialized from YAML (works for both the `cluster`
/// section and standalone upstream files).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RawUpstreamsConfig {
    /// Ignored version field (present in standalone upstream files).
    #[allow(dead_code)]
    pub version: Option<String>,
    #[serde(alias = "defaultOptions")]
    pub defaults: Option<Vec<DefaultOptions>>,
    pub upstreams: Option<Vec<RawUpstream>>,
    pub include: Option<IncludeValue>,
}

/// Processed upstreams configuration.
#[derive(Debug, Clone, Default)]
pub struct UpstreamsConfig {
    pub defaults: Vec<DefaultOptions>,
    pub upstreams: Vec<Upstream>,
}

impl UpstreamsConfig {
    /// Effective options for an upstream: per-chain defaults applied first,
    /// the upstream's own settings on top, library defaults for the rest.
    pub fn options_for(&self, upstream: &Upstream) -> Options {
        let mut merged = PartialOptions::default();
        for defaults in &self.defaults {
            if defaults_apply(defaults, upstream.blockchain.as_deref()) {
                merged = merged.merge(&defaults.resolved_options());
            }
        }
        merged.merge(&upstream.options).build()
    }
}

/// Whether a `defaults` entry applies to an upstream of the given blockchain.
/// An entry must name its blockchains explicitly.
fn defaults_apply(defaults: &DefaultOptions, blockchain: Option<&str>) -> bool {
    let (Some(chains), Some(blockchain)) = (&defaults.blockchains, blockchain) else {
        return false;
    };
    chains.iter().any(|c| chain_name_matches(c, blockchain))
}

/// Compare two blockchain names, accounting for aliases ("ethereum" vs "eth")
/// by parsing both into a [`TargetBlockchain`] when possible.
fn chain_name_matches(a: &str, b: &str) -> bool {
    match (a.parse::<TargetBlockchain>(), b.parse::<TargetBlockchain>()) {
        (Ok(a), Ok(b)) => a == b,
        _ => a.eq_ignore_ascii_case(b),
    }
}

// ─── Processing ──────────────────────────────────────────────────────────────

fn is_valid_upstream_id(id: &str) -> bool {
    id.len() >= 3 && UPSTREAM_ID_RE.is_match(id)
}

/// Converts a raw upstream entry into a processed Upstream.
/// Returns `None` if the upstream has no valid connection or invalid ID.
pub fn process_raw_upstream(raw: RawUpstream) -> Option<Upstream> {
    let id = match &raw.id {
        Some(id) if is_valid_upstream_id(id) => id.clone(),
        Some(id) => {
            warn!("Invalid upstream id: {id}");
            return None;
        }
        None => {
            warn!("Upstream is missing an id");
            return None;
        }
    };

    let connection = match raw.connection {
        Some(conn) => {
            if let Some(eth) = conn.ethereum {
                UpstreamConnection::Ethereum(eth)
            } else if let Some(btc) = conn.bitcoin {
                UpstreamConnection::Bitcoin(btc)
            } else if let Some(ds) = conn.dshackle {
                UpstreamConnection::Dshackle(ds)
            } else {
                warn!("Upstream {id} has no recognized connection type");
                return None;
            }
        }
        None => {
            warn!("Upstream {id} has no connection configured");
            return None;
        }
    };

    let role = raw
        .role
        .as_deref()
        .and_then(|r| {
            let role = UpstreamRole::from_str_lenient(r);
            if role.is_none() {
                warn!("Unsupported role `{r}` for upstream {id}");
            }
            role
        })
        .unwrap_or_default();

    // Merge direct option fields with nested `options` block
    let direct_options = PartialOptions {
        disable_validation: raw.disable_validation,
        validate_syncing: raw.validate_syncing,
        validate_peers: raw.validate_peers,
        min_peers: raw.min_peers,
        validation_interval: raw.validation_interval,
        timeout: raw.timeout,
        priority: raw.priority,
        balance: raw.balance,
    };
    let options = match raw.options {
        Some(nested) => nested.merge(&direct_options),
        None => direct_options,
    };

    Some(Upstream {
        id,
        blockchain: raw.blockchain,
        enabled: raw.enabled.unwrap_or(true),
        role,
        labels: raw.labels,
        methods: raw.methods,
        connection,
        options,
    })
}

impl From<RawUpstreamsConfig> for UpstreamsConfig {
    fn from(raw: RawUpstreamsConfig) -> Self {
        let defaults = raw.defaults.unwrap_or_default();
        let upstreams = raw
            .upstreams
            .unwrap_or_default()
            .into_iter()
            .filter_map(process_raw_upstream)
            .collect();
        UpstreamsConfig {
            defaults,
            upstreams,
        }
    }
}

// ─── Label deserialization ───────────────────────────────────────────────────

/// Custom deserializer for labels: YAML values can be strings or booleans.
fn deserialize_labels<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let map: HashMap<String, serde_yaml::Value> = HashMap::deserialize(deserializer)?;
    Ok(map
        .into_iter()
        .map(|(k, v)| {
            let s = match &v {
                serde_yaml::Value::String(s) => s.clone(),
                serde_yaml::Value::Bool(b) => b.to_string(),
                serde_yaml::Value::Number(n) => n.to_string(),
                _ => format!("{v:?}"),
            };
            (k, s)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Options resolution ───────────────────────────────────────────────

    #[test]
    fn options_defaults_match_legacy() {
        let options = PartialOptions::default().build();

        assert!(!options.disable_validation);
        assert!(options.validate_syncing);
        assert!(options.validate_peers);
        assert_eq!(options.min_peers, 1);
        assert_eq!(options.validation_interval, Duration::from_secs(30));
        assert_eq!(options.timeout, Duration::from_secs(60));
        assert_eq!(options.priority, 10);
        assert!(!options.balance);
    }

    #[test]
    fn options_build_uses_configured_values() {
        let options = PartialOptions {
            disable_validation: Some(true),
            min_peers: Some(5),
            validation_interval: Some(10),
            timeout: Some(20),
            ..Default::default()
        }
        .build();

        assert!(options.disable_validation);
        assert_eq!(options.min_peers, 5);
        assert_eq!(options.validation_interval, Duration::from_secs(10));
        assert_eq!(options.timeout, Duration::from_secs(20));
    }

    fn upstream_for(blockchain: &str, options: PartialOptions) -> Upstream {
        Upstream {
            id: "test-up".to_string(),
            blockchain: Some(blockchain.to_string()),
            enabled: true,
            role: UpstreamRole::Primary,
            labels: HashMap::new(),
            methods: None,
            connection: UpstreamConnection::Ethereum(EthereumConnection {
                rpc: None,
                ws: None,
            }),
            options,
        }
    }

    fn chain_defaults(blockchains: &[&str], min_peers: i32) -> DefaultOptions {
        DefaultOptions {
            blockchains: Some(blockchains.iter().map(|s| s.to_string()).collect()),
            options: Some(PartialOptions {
                min_peers: Some(min_peers),
                ..Default::default()
            }),
            min_peers: None,
            validate_peers: None,
            validate_syncing: None,
            validation_interval: None,
            timeout: None,
        }
    }

    #[test]
    fn chain_defaults_apply_to_matching_upstream() {
        let config = UpstreamsConfig {
            defaults: vec![chain_defaults(&["ethereum"], 7)],
            upstreams: vec![],
        };

        let options = config.options_for(&upstream_for("ethereum", PartialOptions::default()));
        assert_eq!(options.min_peers, 7);
    }

    #[test]
    fn chain_defaults_skip_other_chains() {
        let config = UpstreamsConfig {
            defaults: vec![chain_defaults(&["bitcoin"], 7)],
            upstreams: vec![],
        };

        let options = config.options_for(&upstream_for("ethereum", PartialOptions::default()));
        assert_eq!(options.min_peers, 1);
    }

    #[test]
    fn upstream_options_override_chain_defaults() {
        let config = UpstreamsConfig {
            defaults: vec![chain_defaults(&["ethereum"], 7)],
            upstreams: vec![],
        };

        let upstream = upstream_for(
            "ethereum",
            PartialOptions {
                min_peers: Some(3),
                ..Default::default()
            },
        );
        assert_eq!(config.options_for(&upstream).min_peers, 3);
    }

    // ── ID validation ────────────────────────────────────────────────────

    #[test]
    fn accepts_valid_ids() {
        assert!(is_valid_upstream_id("test"));
        assert!(is_valid_upstream_id("local"));
        assert!(is_valid_upstream_id("infura-eth"));
        assert!(is_valid_upstream_id("my_upstream_1"));
        assert!(is_valid_upstream_id("upstream-01"));
        assert!(is_valid_upstream_id("a1b"));
    }

    #[test]
    fn rejects_invalid_ids() {
        // too short
        assert!(!is_valid_upstream_id("ab"));
        assert!(!is_valid_upstream_id("a"));
        assert!(!is_valid_upstream_id(""));
        // starts with non-letter
        assert!(!is_valid_upstream_id("1test"));
        assert!(!is_valid_upstream_id("-test"));
        // ends with invalid char
        assert!(!is_valid_upstream_id("test-"));
        assert!(!is_valid_upstream_id("test_"));
        // contains slash
        assert!(!is_valid_upstream_id("test/test"));
        // contains special chars
        assert!(!is_valid_upstream_id("test.test"));
        assert!(!is_valid_upstream_id("test@test"));
    }

    // ── PartialOptions merge ─────────────────────────────────────────────

    #[test]
    fn merge_prefers_overwrite_values() {
        let base = PartialOptions {
            min_peers: Some(3),
            disable_validation: Some(false),
            priority: Some(10),
            ..Default::default()
        };
        let overwrite = PartialOptions {
            min_peers: Some(7),
            validate_syncing: Some(true),
            ..Default::default()
        };
        let merged = base.merge(&overwrite);

        assert_eq!(merged.min_peers, Some(7)); // overwritten
        assert_eq!(merged.disable_validation, Some(false)); // kept from base
        assert_eq!(merged.validate_syncing, Some(true)); // from overwrite
        assert_eq!(merged.priority, Some(10)); // kept from base
        assert_eq!(merged.timeout, None); // both None
    }

    #[test]
    fn merge_disable_validation() {
        let base = PartialOptions {
            disable_validation: Some(false),
            ..Default::default()
        };
        let overwrite = PartialOptions {
            disable_validation: Some(true),
            ..Default::default()
        };
        assert_eq!(base.merge(&overwrite).disable_validation, Some(true));
    }

    #[test]
    fn merge_balance() {
        let base = PartialOptions::default();
        let overwrite = PartialOptions {
            balance: Some(true),
            ..Default::default()
        };
        assert_eq!(base.merge(&overwrite).balance, Some(true));
    }

    #[test]
    fn merge_validate_peers() {
        let base = PartialOptions {
            validate_peers: Some(true),
            ..Default::default()
        };
        let overwrite = PartialOptions {
            validate_peers: Some(false),
            ..Default::default()
        };
        assert_eq!(base.merge(&overwrite).validate_peers, Some(false));
    }

    #[test]
    fn merge_validate_syncing() {
        let base = PartialOptions {
            validate_syncing: Some(true),
            ..Default::default()
        };
        let overwrite = PartialOptions::default();
        // None doesn't override
        assert_eq!(base.merge(&overwrite).validate_syncing, Some(true));
    }

    #[test]
    fn merge_timeout() {
        let base = PartialOptions {
            timeout: Some(60),
            ..Default::default()
        };
        let overwrite = PartialOptions {
            timeout: Some(30),
            ..Default::default()
        };
        assert_eq!(base.merge(&overwrite).timeout, Some(30));
    }

    #[test]
    fn merge_priority() {
        let base = PartialOptions {
            priority: Some(10),
            ..Default::default()
        };
        let overwrite = PartialOptions {
            priority: Some(100),
            ..Default::default()
        };
        assert_eq!(base.merge(&overwrite).priority, Some(100));
    }

    #[test]
    fn merge_min_peers() {
        let base = PartialOptions {
            min_peers: Some(1),
            ..Default::default()
        };
        let overwrite = PartialOptions {
            min_peers: Some(5),
            ..Default::default()
        };
        assert_eq!(base.merge(&overwrite).min_peers, Some(5));
    }

    // ── UpstreamRole ─────────────────────────────────────────────────────

    #[test]
    fn role_from_str_lenient() {
        assert_eq!(UpstreamRole::from_str_lenient("primary"), Some(UpstreamRole::Primary));
        assert_eq!(UpstreamRole::from_str_lenient("standard"), Some(UpstreamRole::Primary));
        assert_eq!(UpstreamRole::from_str_lenient("secondary"), Some(UpstreamRole::Secondary));
        assert_eq!(UpstreamRole::from_str_lenient("fallback"), Some(UpstreamRole::Fallback));
        assert_eq!(UpstreamRole::from_str_lenient("FALLBACK"), Some(UpstreamRole::Fallback));
        assert_eq!(UpstreamRole::from_str_lenient("unknown"), None);
    }
}
