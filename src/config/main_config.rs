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

//! Top-level application configuration and YAML file reading.

use crate::config::cache::CacheConfig;
use crate::config::env;
use crate::config::health::HealthConfig;
use crate::config::log::{AccessLogConfig, RequestLogConfig};
use crate::config::monitoring::MonitoringConfig;
use crate::config::proxy::ProxyConfig;
use crate::config::signature::SignatureConfig;
use crate::config::tls::ServerTlsConfig;
use crate::config::tokens::TokenConfig;
use crate::config::upstreams::{RawUpstreamsConfig, UpstreamsConfig};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// The top-level Dshackle configuration.
#[derive(Debug, Clone)]
pub struct MainConfig {
    pub host: String,
    pub port: u16,
    pub tls: Option<ServerTlsConfig>,
    pub compress: bool,
    pub cache: Option<CacheConfig>,
    pub proxy: Option<ProxyConfig>,
    pub upstreams: Option<UpstreamsConfig>,
    pub tokens: Vec<TokenConfig>,
    pub monitoring: MonitoringConfig,
    pub access_log: AccessLogConfig,
    pub request_log: RequestLogConfig,
    pub health: HealthConfig,
    pub signature: Option<SignatureConfig>,
    /// Directory of the config file itself; relative paths mentioned in the
    /// config (TLS certificates, keys) are resolved against it, same as the
    /// legacy FileResolver did.
    pub config_dir: PathBuf,
}

/// Raw YAML structure that maps directly to the config file format.
#[derive(Debug, Deserialize)]
struct RawMainConfig {
    #[allow(dead_code)]
    version: Option<String>,
    #[serde(default = "default_host")]
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default = "default_compress")]
    compress: bool,
    tls: Option<ServerTlsConfig>,
    cache: Option<CacheConfig>,
    proxy: Option<ProxyConfig>,
    tokens: Option<Vec<TokenConfig>>,
    #[serde(default)]
    monitoring: MonitoringConfig,
    #[serde(
        default,
        alias = "accessLog",
        alias = "egress-log",
        alias = "egressLog"
    )]
    #[serde(rename = "access-log")]
    access_log: AccessLogConfig,
    #[serde(
        default,
        alias = "requestLog",
        alias = "ingress-log",
        alias = "ingressLog"
    )]
    #[serde(rename = "request-log")]
    request_log: RequestLogConfig,
    #[serde(default)]
    health: HealthConfig,
    #[serde(alias = "signedResponse", rename = "signed-response")]
    signature: Option<SignatureConfig>,
    cluster: Option<RawUpstreamsConfig>,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    2449
}

fn default_compress() -> bool {
    true
}

/// Reads and parses a Dshackle configuration file.
///
/// Performs environment variable substitution, processes upstream includes,
/// and resolves default options.
pub fn read_config(path: &Path) -> Result<MainConfig> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("reading config: {}", path.display()))?;

    let config_dir = path.parent().unwrap_or(Path::new("."));
    parse_main_config(&content, config_dir)
}

/// Parses a main config from a YAML string. `config_dir` is used to resolve
/// relative include paths.
fn parse_main_config(yaml: &str, config_dir: &Path) -> Result<MainConfig> {
    let mut value: serde_yaml::Value = serde_yaml::from_str(yaml).context("parsing YAML")?;
    env::substitute_in_yaml(&mut value);

    let raw: RawMainConfig = serde_yaml::from_value(value).context("deserializing config")?;

    // Process upstreams (cluster section) with file includes
    let upstreams = match raw.cluster {
        Some(raw_cluster) => {
            let mut config = UpstreamsConfig::try_from(raw_cluster.clone())?;

            // Handle file includes
            if let Some(include) = raw_cluster.include {
                let paths: Vec<String> = include.into();
                for include_path in paths {
                    let resolved = crate::config::resolve_file(config_dir, &include_path);
                    // A missing include file is tolerated (legacy parity for
                    // optional includes); one that exists but can't be read
                    // or parsed is a hard error — silently dropping its
                    // upstreams would be worse.
                    if !resolved.exists() || !resolved.is_file() {
                        warn!("Included config not accessible: {}", resolved.display());
                        continue;
                    }
                    let included = read_included_upstreams(&resolved)?;
                    config.upstreams.extend(included.upstreams);
                }
            }

            Some(config)
        }
        None => None,
    };

    Ok(MainConfig {
        host: raw.host,
        port: raw.port,
        tls: raw.tls,
        compress: raw.compress,
        cache: raw.cache,
        proxy: raw.proxy,
        upstreams,
        tokens: raw.tokens.unwrap_or_default(),
        monitoring: raw.monitoring,
        access_log: raw.access_log,
        request_log: raw.request_log,
        health: raw.health,
        signature: raw.signature,
        config_dir: config_dir.to_path_buf(),
    })
}

/// Reads an included upstream config file.
fn read_included_upstreams(path: &Path) -> Result<UpstreamsConfig> {
    info!("Including upstream config: {}", path.display());
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("reading included config: {}", path.display()))?;

    content
        .parse()
        .with_context(|| format!("parsing included config {}", path.display()))
}

impl std::str::FromStr for UpstreamsConfig {
    type Err = anyhow::Error;

    fn from_str(yaml: &str) -> Result<Self> {
        let mut value: serde_yaml::Value = serde_yaml::from_str(yaml).context("parsing YAML")?;
        env::substitute_in_yaml(&mut value);

        let raw: RawUpstreamsConfig =
            serde_yaml::from_value(value).context("deserializing upstreams config")?;

        raw.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::{UpstreamConnection, UpstreamRole};
    use std::path::PathBuf;

    fn testdata(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("testdata/configs")
            .join(name)
    }

    // ── Full config ──────────────────────────────────────────────────────

    #[test]
    fn parse_full_config() {
        let cfg = read_config(&testdata("dshackle-full.yaml")).unwrap();

        assert_eq!(cfg.host, "192.168.1.101");
        assert_eq!(cfg.port, 2448);

        // TLS
        let tls = cfg.tls.as_ref().unwrap();
        assert_eq!(tls.enabled, Some(true));
        let server = tls.server.as_ref().unwrap();
        assert_eq!(server.certificate, "/path/127.0.0.1.crt");
        assert_eq!(server.key, "/path/127.0.0.1.p8.key");
        let client = tls.client.as_ref().unwrap();
        assert!(!client.require);

        // Cache
        let cache = cfg.cache.as_ref().unwrap();
        let redis = cache.redis.as_ref().unwrap();
        assert!(redis.enabled);
        assert_eq!(redis.host, "redis-master");

        // Proxy
        let proxy = cfg.proxy.as_ref().unwrap();
        assert_eq!(proxy.port, 8082);
        assert_eq!(proxy.routes.len(), 5);
        assert_eq!(proxy.routes[0].id, "eth");
        assert_eq!(proxy.routes[0].blockchain, "ethereum");

        // Health
        assert_eq!(cfg.health.port, 8083);
        assert_eq!(cfg.health.blockchains.len(), 1);
        assert_eq!(cfg.health.blockchains[0].blockchain, "ethereum");

        // Upstreams (including the included file)
        let upstreams = cfg.upstreams.as_ref().unwrap();
        assert!(upstreams.upstreams.len() >= 3); // local + infura + remote (from include)

        let local = upstreams
            .upstreams
            .iter()
            .find(|u| u.id == "local")
            .unwrap();
        assert_eq!(local.blockchain.as_deref(), Some("ethereum"));

        let remote = upstreams
            .upstreams
            .iter()
            .find(|u| u.id == "remote")
            .unwrap();
        assert!(matches!(remote.connection, UpstreamConnection::Dshackle(_)));
    }

    // ── Cache configs ────────────────────────────────────────────────────

    #[test]
    fn parse_cache_redis_full() {
        let cfg = read_config(&testdata("cache-redis-full.yaml")).unwrap();
        let redis = cfg.cache.unwrap().redis.unwrap();
        assert!(redis.enabled);
        assert_eq!(redis.host, "redis-master");
        assert_eq!(redis.port, 1234);
        assert_eq!(redis.db, 5);
        assert_eq!(redis.password.as_deref(), Some("HelloWorld!1"));
    }

    #[test]
    fn parse_cache_redis_disabled() {
        let cfg = read_config(&testdata("cache-redis-disabled.yaml")).unwrap();
        let redis = cfg.cache.unwrap().redis.unwrap();
        assert!(!redis.enabled);
    }

    // ── Health configs ───────────────────────────────────────────────────

    #[test]
    fn parse_health_empty() {
        let cfg = read_config(&testdata("dshackle-health-empty.yaml")).unwrap();
        assert!(cfg.health.blockchains.is_empty());
        assert_eq!(cfg.health.port, 8082); // default
    }

    #[test]
    fn parse_health_one_chain() {
        let cfg = read_config(&testdata("dshackle-health-1.yaml")).unwrap();
        assert_eq!(cfg.health.blockchains.len(), 1);
        assert_eq!(cfg.health.blockchains[0].blockchain, "ethereum");
        assert_eq!(cfg.health.blockchains[0].min_available, 2);
    }

    #[test]
    fn parse_health_two_chains() {
        let cfg = read_config(&testdata("dshackle-health-2.yaml")).unwrap();
        assert_eq!(cfg.health.port, 10003);
        assert_eq!(cfg.health.host, "0.0.0.0");
        assert_eq!(cfg.health.path, "/healtz");
        assert_eq!(cfg.health.blockchains.len(), 2);
        // second entry uses `chain` alias
        assert_eq!(cfg.health.blockchains[1].blockchain, "bitcoin");
    }

    // ── Proxy configs ────────────────────────────────────────────────────

    #[test]
    fn parse_proxy_basic() {
        let cfg = read_config(&testdata("dshackle-proxy-basic.yaml")).unwrap();
        let proxy = cfg.proxy.unwrap();
        assert!(proxy.enabled);
        assert!(proxy.websocket); // default true
        assert_eq!(proxy.host, "127.0.0.1"); // default
        assert_eq!(proxy.port, 8080);
        assert_eq!(proxy.routes.len(), 1);
        assert_eq!(proxy.routes[0].id, "ethereum");
        assert_eq!(proxy.routes[0].blockchain, "ethereum");
    }

    #[test]
    fn parse_proxy_no_ws() {
        let cfg = read_config(&testdata("dshackle-proxy-no-ws.yaml")).unwrap();
        let proxy = cfg.proxy.unwrap();
        assert!(!proxy.websocket);
    }

    #[test]
    fn parse_proxy_two_routes() {
        let cfg = read_config(&testdata("dshackle-proxy-two.yaml")).unwrap();
        let proxy = cfg.proxy.unwrap();
        assert_eq!(proxy.routes.len(), 2);
        assert_eq!(proxy.routes[1].id, "classic");
    }

    #[test]
    fn parse_proxy_max() {
        let cfg = read_config(&testdata("dshackle-proxy-max.yaml")).unwrap();
        let proxy = cfg.proxy.unwrap();
        assert!(proxy.enabled);
        assert_eq!(proxy.host, "0.0.0.0");
        assert_eq!(proxy.port, 8080);
        assert!(proxy.preserve_batch_order);
        assert_eq!(proxy.routes.len(), 2);
        assert_eq!(proxy.cors_origin.as_deref(), Some("*"));
        assert_eq!(proxy.cors_allowed_headers.as_deref(), Some("Content-Type"));
    }

    // ── Monitoring ───────────────────────────────────────────────────────

    #[test]
    fn parse_monitoring_basic() {
        let cfg = read_config(&testdata("dshackle-monitoring-basic.yaml")).unwrap();
        assert!(cfg.monitoring.enabled);
        assert!(cfg.monitoring.prometheus.enabled);
        assert_eq!(cfg.monitoring.prometheus.bind, "192.168.0.1");
        assert_eq!(cfg.monitoring.prometheus.port, 8000);
        assert_eq!(cfg.monitoring.prometheus.path, "/status/prometheus");
    }

    // ── Upstream configs ─────────────────────────────────────────────────

    #[test]
    fn parse_upstreams_basic() {
        let yaml = std::fs::read_to_string(testdata("upstreams-basic.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        assert_eq!(cfg.defaults.len(), 1);
        assert_eq!(cfg.upstreams.len(), 2);

        let local = &cfg.upstreams[0];
        assert_eq!(local.id, "local");
        assert_eq!(local.blockchain.as_deref(), Some("ethereum"));
        if let UpstreamConnection::Ethereum(eth) = &local.connection {
            let rpc = eth.rpc.as_ref().unwrap();
            assert_eq!(rpc.url, "http://localhost:8545");

            let ws = eth.ws.as_ref().unwrap();
            assert_eq!(ws.url, "ws://localhost:8546");
            assert_eq!(ws.origin.as_deref(), Some("http://localhost"));
            let auth = ws.basic_auth.as_ref().unwrap();
            assert_eq!(auth.username, "9c199ad8f281f20154fc258fe41a6814");
            assert_eq!(auth.password, "258fe4149c199ad8f2811a68f20154fc");
        } else {
            panic!("Expected Ethereum connection");
        }

        let infura = &cfg.upstreams[1];
        assert_eq!(infura.id, "infura");
        if let UpstreamConnection::Ethereum(eth) = &infura.connection {
            let rpc = eth.rpc.as_ref().unwrap();
            assert_eq!(
                rpc.url,
                "https://mainnet.infura.io/v3/fa28c968191849c1aff541ad1d8511f2"
            );
            assert!(rpc.basic_auth.is_some());
        } else {
            panic!("Expected Ethereum connection");
        }
    }

    #[test]
    fn parse_upstreams_bitcoin() {
        let yaml = std::fs::read_to_string(testdata("upstreams-bitcoin.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        assert_eq!(cfg.upstreams.len(), 1);
        assert!(matches!(
            cfg.upstreams[0].connection,
            UpstreamConnection::Bitcoin(_)
        ));
    }

    #[test]
    fn parse_upstreams_bitcoin_esplora() {
        let yaml = std::fs::read_to_string(testdata("upstreams-bitcoin-esplora.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        assert_eq!(cfg.upstreams[0].options.balance, Some(true));
        if let UpstreamConnection::Bitcoin(btc) = &cfg.upstreams[0].connection {
            assert!(btc.rpc.is_some());
            assert_eq!(btc.rpc.as_ref().unwrap().url, "http://localhost:8545");
            assert!(btc.esplora.is_some());
            assert_eq!(btc.esplora.as_ref().unwrap().url, "http://localhost:3001");
        } else {
            panic!("Expected Bitcoin connection");
        }
    }

    #[test]
    fn parse_upstreams_bitcoin_zmq() {
        let yaml = std::fs::read_to_string(testdata("upstreams-bitcoin-zmq.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        assert_eq!(cfg.upstreams.len(), 2);
        if let UpstreamConnection::Bitcoin(btc) = &cfg.upstreams[0].connection {
            assert_eq!(btc.zeromq.as_ref().unwrap().url, "tcp://191.168.1.5:1234");
        } else {
            panic!("Expected Bitcoin connection");
        }
    }

    #[test]
    fn parse_upstreams_dshackle() {
        let yaml = std::fs::read_to_string(testdata("upstreams-ds.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        assert_eq!(cfg.upstreams.len(), 2);
        let internal = &cfg.upstreams[0];
        assert_eq!(internal.id, "internal");
        if let UpstreamConnection::Dshackle(ds) = &internal.connection {
            assert_eq!(ds.host.as_deref(), Some("10.2.0.15"));
            let tls = ds.tls.as_ref().unwrap();
            assert_eq!(tls.ca.as_deref(), Some("/etc/ca.myservice.com.crt"));
            assert_eq!(
                tls.certificate.as_deref(),
                Some("/etc/client1.myservice.com.crt")
            );
            assert_eq!(tls.key.as_deref(), Some("/etc/client1.myservice.com.key"));
        } else {
            panic!("Expected Dshackle connection");
        }

        // Second uses `url` style
        let public = &cfg.upstreams[1];
        assert_eq!(public.id, "public");
        if let UpstreamConnection::Dshackle(ds) = &public.connection {
            assert_eq!(ds.url.as_deref(), Some("https://rpc.provider.io/"));
        } else {
            panic!("Expected Dshackle connection");
        }
    }

    #[test]
    fn parse_upstreams_roles() {
        let yaml = std::fs::read_to_string(testdata("upstreams-roles.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        assert_eq!(cfg.upstreams.len(), 2);
        assert_eq!(cfg.upstreams[0].role, UpstreamRole::Primary); // default
        assert_eq!(cfg.upstreams[1].role, UpstreamRole::Fallback);
    }

    #[test]
    fn parse_upstreams_roles_2() {
        let yaml = std::fs::read_to_string(testdata("upstreams-roles-2.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        assert_eq!(cfg.upstreams.len(), 3);
        assert_eq!(cfg.upstreams[0].role, UpstreamRole::Primary);
        assert_eq!(cfg.upstreams[1].role, UpstreamRole::Secondary);
        assert_eq!(cfg.upstreams[2].role, UpstreamRole::Fallback);
    }

    #[test]
    fn parse_upstreams_invalid_role_falls_back() {
        let yaml = std::fs::read_to_string(testdata("upstreams-roles-invalid.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        // Invalid role "fallbackZ" → default Primary
        let infura = cfg.upstreams.iter().find(|u| u.id == "infura").unwrap();
        assert_eq!(infura.role, UpstreamRole::Primary);
    }

    #[test]
    fn parse_upstreams_labels() {
        let yaml = std::fs::read_to_string(testdata("upstreams-labels.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        let local = cfg.upstreams.iter().find(|u| u.id == "local").unwrap();
        assert_eq!(local.labels.get("api"), Some(&"geth".to_string()));
        assert_eq!(local.labels.get("fullnode"), Some(&"true".to_string()));
    }

    #[test]
    fn parse_upstreams_methods() {
        let yaml = std::fs::read_to_string(testdata("upstreams-methods.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        let local = &cfg.upstreams[0];
        let methods = local.methods.as_ref().unwrap();
        assert_eq!(methods.enabled.len(), 1);
        assert_eq!(methods.enabled[0].name, "parity_trace");
        assert_eq!(methods.disabled.len(), 2);
        assert_eq!(methods.disabled[0].name, "eth_getBlockByNumber");
        assert_eq!(methods.disabled[1].name, "admin_shutdown");
    }

    #[test]
    fn parse_upstreams_methods_quorum() {
        let yaml = std::fs::read_to_string(testdata("upstreams-methods-quorum.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        let methods = cfg.upstreams[0].methods.as_ref().unwrap();
        assert_eq!(methods.enabled[0].name, "custom_foo");
        assert_eq!(methods.enabled[0].quorum.as_deref(), Some("not_lagging"));
        assert_eq!(methods.enabled[1].name, "custom_bar");
        assert_eq!(methods.enabled[1].quorum.as_deref(), Some("not_empty"));
    }

    #[test]
    fn parse_upstreams_options() {
        let yaml = std::fs::read_to_string(testdata("upstreams-options.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        // First upstream has options.min-peers = 7
        let local = &cfg.upstreams[0];
        assert_eq!(local.options.min_peers, Some(7));

        // Second upstream has disable-validation = true (direct field)
        let infura = &cfg.upstreams[1];
        assert_eq!(infura.options.disable_validation, Some(true));
    }

    #[test]
    fn parse_upstreams_validation() {
        let yaml = std::fs::read_to_string(testdata("upstreams-validation.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        let t1 = cfg.upstreams.iter().find(|u| u.id == "test-1").unwrap();
        assert_eq!(t1.options.disable_validation, Some(false));
        assert_eq!(t1.options.validate_syncing, Some(true));
        assert_eq!(t1.options.validate_peers, Some(false));

        let t3 = cfg.upstreams.iter().find(|u| u.id == "test-3").unwrap();
        assert_eq!(t3.options.disable_validation, Some(true));
    }

    #[test]
    fn parse_upstreams_priority() {
        let yaml = std::fs::read_to_string(testdata("upstreams-priority.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        let local = cfg.upstreams.iter().find(|u| u.id == "local").unwrap();
        assert_eq!(local.options.priority, Some(100));

        let infura = cfg.upstreams.iter().find(|u| u.id == "infura").unwrap();
        assert_eq!(infura.options.priority, Some(50));
        assert_eq!(infura.role, UpstreamRole::Fallback);

        let remote = cfg.upstreams.iter().find(|u| u.id == "remote").unwrap();
        assert_eq!(remote.options.priority, Some(75));
    }

    #[test]
    fn parse_upstreams_ws_full() {
        let yaml = std::fs::read_to_string(testdata("upstreams-ws-full.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        if let UpstreamConnection::Ethereum(eth) = &cfg.upstreams[0].connection {
            let ws = eth.ws.as_ref().unwrap();
            assert_eq!(ws.frame_size, Some(10 * 1024 * 1024));
            assert_eq!(ws.msg_size, Some(25 * 1024 * 1024));
        } else {
            panic!("Expected Ethereum connection");
        }
    }

    #[test]
    fn parse_upstreams_ws_only() {
        let yaml = std::fs::read_to_string(testdata("upstreams-ws-only.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        if let UpstreamConnection::Ethereum(eth) = &cfg.upstreams[0].connection {
            assert!(eth.rpc.is_none());
            assert!(eth.ws.is_some());
        } else {
            panic!("Expected Ethereum connection");
        }
    }

    #[test]
    fn parse_upstreams_no_defaults() {
        let yaml = std::fs::read_to_string(testdata("upstreams-no-defaults.yaml")).unwrap();
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();

        assert!(cfg.defaults.is_empty());
        assert_eq!(cfg.upstreams.len(), 1);
    }

    #[test]
    fn parse_upstreams_missing_id_fails() {
        // The file carries an enabled upstream without an id — a config error
        // that must stop the startup, not be silently skipped (which is what
        // legacy did). Malformed ids (the file also has "test/test") are
        // caught later, at wiring, so disabled entries never fail on them.
        let yaml = std::fs::read_to_string(testdata("upstreams-no-id.yaml")).unwrap();
        assert!(yaml.parse::<UpstreamsConfig>().is_err());
    }

    #[test]
    fn duplicate_label_names_collapse_deterministically() {
        // Two YAML keys trimming to the same name must resolve by document
        // order (the later wins, as legacy's map insert did) — not by random
        // `HashMap` iteration.
        let yaml = r#"
upstreams:
  - id: labeled
    blockchain: ethereum
    labels:
      archive: "false"
      " archive ": "true"
    connection:
      ethereum:
        rpc:
          url: "http://localhost:8545"
"#;
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();
        let labels = cfg.upstreams[0].upstream_labels().unwrap();
        assert_eq!(labels.get("archive"), Some("true"));
        assert_eq!(labels.iter().count(), 1);
    }

    // ── Tokens ───────────────────────────────────────────────────────────

    #[test]
    fn parse_tokens() {
        use crate::config::tokens::TokenType;
        let yaml = r#"
tokens:
  - id: dai
    blockchain: ethereum
    name: DAI
    type: ERC-20
    address: 0x6B175474E89094C44Da98b954EedeAC495271d0F
  - id: tether
    blockchain: ethereum
    name: Tether
    type: ERC-20
    address: 0xdac17f958d2ee523a2206206994597c13d831ec7
"#;
        let cfg = parse_main_config(yaml, Path::new(".")).unwrap();
        assert_eq!(cfg.tokens.len(), 2);

        assert_eq!(cfg.tokens[0].id, "dai");
        assert_eq!(cfg.tokens[0].blockchain, "ethereum");
        assert_eq!(cfg.tokens[0].name, "DAI");
        assert_eq!(cfg.tokens[0].token_type, TokenType::Erc20);
        assert_eq!(
            cfg.tokens[0].address,
            "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        );

        assert_eq!(cfg.tokens[1].id, "tether");
        assert_eq!(cfg.tokens[1].name, "Tether");
        assert_eq!(cfg.tokens[1].token_type, TokenType::Erc20);
        assert_eq!(
            cfg.tokens[1].address,
            "0xdac17f958d2ee523a2206206994597c13d831ec7"
        );
    }

    // ── Proxy routes ─────────────────────────────────────────────────────

    #[test]
    fn parse_proxy_two_routes_blockchain_values() {
        let cfg = read_config(&testdata("dshackle-proxy-two.yaml")).unwrap();
        let proxy = cfg.proxy.unwrap();
        assert_eq!(proxy.routes[0].id, "ethereum");
        assert_eq!(proxy.routes[0].blockchain, "ethereum");
        assert_eq!(proxy.routes[1].id, "classic");
        assert_eq!(proxy.routes[1].blockchain, "etc");
    }

    // ── Defaults ─────────────────────────────────────────────────────────

    #[test]
    fn defaults_for_minimal_config() {
        let yaml = "version: v1\n";
        let cfg = parse_main_config(yaml, Path::new(".")).unwrap();
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 2449);
        assert!(cfg.compress);
        assert!(cfg.tls.is_none());
        assert!(cfg.cache.is_none());
        assert!(cfg.proxy.is_none());
        assert!(cfg.upstreams.is_none());
        assert!(cfg.tokens.is_empty());
        assert!(cfg.monitoring.enabled);
        assert!(!cfg.access_log.enabled);
        assert!(!cfg.request_log.enabled);
        assert!(!cfg.health.is_enabled());
        assert!(cfg.signature.is_none());
    }

    // ── Access / Request log from main config ────────────────────────────

    #[test]
    fn parse_access_log_config() {
        let yaml = r#"
access-log:
  enabled: true
  include-messages: true
  filename: /var/log/dshackle/access_log.jsonl
"#;
        let cfg = parse_main_config(yaml, Path::new(".")).unwrap();
        assert!(cfg.access_log.enabled);
        assert!(cfg.access_log.include_messages);
        assert_eq!(
            cfg.access_log.target.filename.as_deref(),
            Some("/var/log/dshackle/access_log.jsonl")
        );
    }

    #[test]
    fn parse_request_log_config() {
        let yaml = r#"
request-log:
  enabled: true
  include-params: true
  filename: /var/log/dshackle/request_log.jsonl
"#;
        let cfg = parse_main_config(yaml, Path::new(".")).unwrap();
        assert!(cfg.request_log.enabled);
        assert!(cfg.request_log.include_params);
        assert_eq!(
            cfg.request_log.target.filename.as_deref(),
            Some("/var/log/dshackle/request_log.jsonl")
        );
    }

    // ── WS connection details ────────────────────────────────────────────

    #[test]
    fn parse_upstreams_ws_connection_options() {
        let yaml = r#"
version: v1
upstreams:
  - id: local-ws
    blockchain: ethereum
    connection:
      ethereum:
        rpc:
          url: "http://localhost:8545"
          compress: true
        ws:
          url: "ws://localhost:8546"
          connections: 4
          compress: true
          disable-methods:
            - trace_block
            - trace_transaction
"#;
        let cfg = yaml.parse::<UpstreamsConfig>().unwrap();
        if let UpstreamConnection::Ethereum(eth) = &cfg.upstreams[0].connection {
            let rpc = eth.rpc.as_ref().unwrap();
            assert_eq!(rpc.compress, Some(true));

            let ws = eth.ws.as_ref().unwrap();
            assert_eq!(ws.connections, Some(4));
            assert_eq!(ws.compress, Some(true));
            assert_eq!(ws.disabled_methods.len(), 2);
            assert_eq!(ws.disabled_methods[0], "trace_block");
            assert_eq!(ws.disabled_methods[1], "trace_transaction");
        } else {
            panic!("Expected Ethereum connection");
        }
    }
}
