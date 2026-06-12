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

//! Ethereum upstream validation, the port of the legacy
//! `EthereumUpstreamValidator` plus a chain-id check.
//!
//! Three probes, each contributing an availability verdict; the worst one
//! wins:
//! - `eth_chainId` — the upstream must serve the configured blockchain. A
//!   misconfigured upstream would otherwise poison responses and caches with
//!   data from another chain. Checked until it passes once: the chain id of
//!   a node never changes, so a confirmed upstream skips the probe.
//! - `eth_syncing` (when `validate-syncing`) — a node performing initial
//!   sync answers from an incomplete state.
//! - `net_peerCount` vs `min-peers` (when `validate-peers`) — a node with
//!   too few peers may be isolated from the network.
//!
//! Any probe failure — timeout, connection error, JSON-RPC error — makes the
//! upstream `Unavailable`, which is what removes upstreams with connection
//! issues from the rotation.

use crate::blockchain::TargetBlockchain;
use crate::config::upstreams::Options;
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::ethereum::parse_hex_quantity;
use crate::upstream::traits::RpcUpstream;
use crate::upstream::validation::{probe, UpstreamValidator};
use emerald_api::proto::common::ChainRef;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub struct EthereumValidator {
    options: Options,
    expected_chain_id: Option<u64>,
    chain_confirmed: AtomicBool,
}

impl EthereumValidator {
    pub fn new(chain: TargetBlockchain, options: Options) -> Self {
        Self {
            options,
            expected_chain_id: evm_chain_id(chain),
            chain_confirmed: AtomicBool::new(false),
        }
    }

    /// A probe must answer well before the overall request timeout, matching
    /// the legacy `timeoutInternal = timeout / 4`.
    fn probe_timeout(&self) -> Duration {
        self.options.timeout / 4
    }

    async fn validate_chain(&self, upstream: &dyn RpcUpstream) -> UpstreamAvailability {
        let Some(expected) = self.expected_chain_id else {
            return UpstreamAvailability::Ok;
        };
        if self.chain_confirmed.load(Ordering::Relaxed) {
            return UpstreamAvailability::Ok;
        }
        let chain_id = probe(upstream, "eth_chainId", self.probe_timeout())
            .await
            .as_ref()
            .and_then(|v| v.as_str())
            .and_then(parse_hex_quantity);
        match chain_id {
            Some(actual) if actual == expected => {
                self.chain_confirmed.store(true, Ordering::Relaxed);
                UpstreamAvailability::Ok
            }
            Some(actual) => {
                tracing::error!(
                    upstream = upstream.id(),
                    expected,
                    actual,
                    "upstream serves a different blockchain"
                );
                UpstreamAvailability::Unavailable
            }
            None => UpstreamAvailability::Unavailable,
        }
    }

    async fn validate_syncing(&self, upstream: &dyn RpcUpstream) -> UpstreamAvailability {
        if !self.options.validate_syncing {
            return UpstreamAvailability::Ok;
        }
        match probe(upstream, "eth_syncing", self.probe_timeout()).await {
            Some(serde_json::Value::Bool(false)) => UpstreamAvailability::Ok,
            // `true` or a sync-progress object — the node is syncing
            Some(_) => UpstreamAvailability::Syncing,
            None => UpstreamAvailability::Unavailable,
        }
    }

    async fn validate_peers(&self, upstream: &dyn RpcUpstream) -> UpstreamAvailability {
        if !self.options.validate_peers || self.options.min_peers == 0 {
            return UpstreamAvailability::Ok;
        }
        let count = probe(upstream, "net_peerCount", self.probe_timeout())
            .await
            .as_ref()
            .and_then(|v| v.as_str())
            .and_then(parse_hex_quantity);
        match count {
            Some(count) if count < self.options.min_peers as u64 => {
                UpstreamAvailability::Immature
            }
            Some(_) => UpstreamAvailability::Ok,
            None => UpstreamAvailability::Unavailable,
        }
    }
}

#[async_trait::async_trait]
impl UpstreamValidator for EthereumValidator {
    async fn validate(&self, upstream: &dyn RpcUpstream) -> UpstreamAvailability {
        let chain = self.validate_chain(upstream).await;
        if chain == UpstreamAvailability::Unavailable {
            // Wrong chain (or unreachable) — the other probes can't improve it
            return chain;
        }
        let syncing = self.validate_syncing(upstream).await;
        let peers = self.validate_peers(upstream).await;
        chain.max(syncing).max(peers)
    }
}

/// EVM chain id (as answered by `eth_chainId`) for the supported chains.
/// `None` skips the chain check — there is nothing reliable to compare with.
fn evm_chain_id(chain: TargetBlockchain) -> Option<u64> {
    let TargetBlockchain::Standard(chain) = chain;
    match chain {
        ChainRef::ChainEthereum => Some(1),
        ChainRef::ChainEthereumClassic => Some(61),
        ChainRef::ChainFantom => Some(250),
        ChainRef::ChainMatic => Some(137),
        ChainRef::ChainRsk => Some(30),
        ChainRef::ChainSepolia => Some(11_155_111),
        ChainRef::ChainHoodi => Some(560_048),
        ChainRef::ChainHolesky => Some(17_000),
        ChainRef::ChainGoerli => Some(5),
        ChainRef::ChainRopsten => Some(3),
        ChainRef::ChainRinkeby => Some(4),
        ChainRef::ChainKovan => Some(42),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::PartialOptions;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::UpstreamError;
    use std::collections::HashMap;
    use std::sync::Arc;

    static MOCK_STATE: std::sync::LazyLock<Arc<UpstreamState>> =
        std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));

    /// Answers each method with a canned raw JSON result; methods without an
    /// entry get a transport error.
    struct StubUpstream {
        responses: HashMap<&'static str, &'static str>,
        calls: std::sync::Mutex<Vec<String>>,
    }

    impl StubUpstream {
        fn new(responses: &[(&'static str, &'static str)]) -> Self {
            Self {
                responses: responses.iter().copied().collect(),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }

        /// A node in perfect shape: right chain, synced, well-connected.
        fn healthy() -> Self {
            Self::new(&[
                ("eth_chainId", "\"0x1\""),
                ("eth_syncing", "false"),
                ("net_peerCount", "\"0x19\""),
            ])
        }

        fn calls_of(&self, method: &str) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|m| *m == method)
                .count()
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let method = request.method.as_str();
            self.calls.lock().unwrap().push(method.to_string());
            match self.responses.get(method) {
                Some(result) => {
                    let raw = format!(r#"{{"jsonrpc":"2.0","id":0,"result":{result}}}"#);
                    Ok(serde_json::from_str(&raw).unwrap())
                }
                None => Err(UpstreamError::Transport("refused".into())),
            }
        }
        fn id(&self) -> &str {
            "stub"
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::Ok
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            None
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &MOCK_STATE
        }
    }

    fn ethereum() -> TargetBlockchain {
        ChainRef::ChainEthereum.into()
    }

    fn options(partial: PartialOptions) -> Options {
        partial.build()
    }

    fn default_validator() -> EthereumValidator {
        EthereumValidator::new(ethereum(), options(PartialOptions::default()))
    }

    #[tokio::test]
    async fn healthy_upstream_is_ok() {
        let validator = default_validator();
        let result = validator.validate(&StubUpstream::healthy()).await;
        assert_eq!(result, UpstreamAvailability::Ok);
    }

    #[tokio::test]
    async fn syncing_node_is_syncing() {
        let validator = default_validator();
        let upstream = StubUpstream::new(&[
            ("eth_chainId", "\"0x1\""),
            (
                "eth_syncing",
                r#"{"startingBlock":"0x0","currentBlock":"0x10","highestBlock":"0x100"}"#,
            ),
            ("net_peerCount", "\"0x19\""),
        ]);

        assert_eq!(
            validator.validate(&upstream).await,
            UpstreamAvailability::Syncing
        );
    }

    #[tokio::test]
    async fn too_few_peers_is_immature() {
        let validator = EthereumValidator::new(
            ethereum(),
            options(PartialOptions {
                min_peers: Some(10),
                ..Default::default()
            }),
        );
        let upstream = StubUpstream::new(&[
            ("eth_chainId", "\"0x1\""),
            ("eth_syncing", "false"),
            ("net_peerCount", "\"0x3\""),
        ]);

        assert_eq!(
            validator.validate(&upstream).await,
            UpstreamAvailability::Immature
        );
    }

    #[tokio::test]
    async fn connection_error_is_unavailable() {
        let validator = default_validator();
        let upstream = StubUpstream::new(&[]); // every call fails

        assert_eq!(
            validator.validate(&upstream).await,
            UpstreamAvailability::Unavailable
        );
    }

    #[tokio::test]
    async fn wrong_chain_is_unavailable() {
        let validator = default_validator();
        let upstream = StubUpstream::new(&[
            ("eth_chainId", "\"0x89\""), // Polygon, not Mainnet
            ("eth_syncing", "false"),
            ("net_peerCount", "\"0x19\""),
        ]);

        assert_eq!(
            validator.validate(&upstream).await,
            UpstreamAvailability::Unavailable
        );
        // Wrong chain short-circuits the other probes
        assert_eq!(upstream.calls_of("eth_syncing"), 0);
    }

    #[tokio::test]
    async fn chain_checked_only_until_confirmed() {
        let validator = default_validator();
        let upstream = StubUpstream::healthy();

        validator.validate(&upstream).await;
        validator.validate(&upstream).await;

        assert_eq!(upstream.calls_of("eth_chainId"), 1);
        assert_eq!(upstream.calls_of("eth_syncing"), 2);
    }

    #[tokio::test]
    async fn syncing_check_can_be_disabled() {
        let validator = EthereumValidator::new(
            ethereum(),
            options(PartialOptions {
                validate_syncing: Some(false),
                ..Default::default()
            }),
        );
        let upstream = StubUpstream::new(&[
            ("eth_chainId", "\"0x1\""),
            ("eth_syncing", "true"),
            ("net_peerCount", "\"0x19\""),
        ]);

        assert_eq!(
            validator.validate(&upstream).await,
            UpstreamAvailability::Ok
        );
        assert_eq!(upstream.calls_of("eth_syncing"), 0);
    }

    #[tokio::test]
    async fn peers_check_can_be_disabled() {
        let validator = EthereumValidator::new(
            ethereum(),
            options(PartialOptions {
                validate_peers: Some(false),
                ..Default::default()
            }),
        );
        let upstream = StubUpstream::new(&[
            ("eth_chainId", "\"0x1\""),
            ("eth_syncing", "false"),
        ]);

        assert_eq!(
            validator.validate(&upstream).await,
            UpstreamAvailability::Ok
        );
        assert_eq!(upstream.calls_of("net_peerCount"), 0);
    }

    #[tokio::test]
    async fn min_peers_zero_skips_peer_check() {
        let validator = EthereumValidator::new(
            ethereum(),
            options(PartialOptions {
                min_peers: Some(0),
                ..Default::default()
            }),
        );
        let upstream =
            StubUpstream::new(&[("eth_chainId", "\"0x1\""), ("eth_syncing", "false")]);

        assert_eq!(
            validator.validate(&upstream).await,
            UpstreamAvailability::Ok
        );
    }

    #[tokio::test]
    async fn worst_verdict_wins() {
        // Syncing AND too few peers — Syncing (4) is worse than Immature (3)
        let validator = EthereumValidator::new(
            ethereum(),
            options(PartialOptions {
                min_peers: Some(10),
                ..Default::default()
            }),
        );
        let upstream = StubUpstream::new(&[
            ("eth_chainId", "\"0x1\""),
            ("eth_syncing", "true"),
            ("net_peerCount", "\"0x1\""),
        ]);

        assert_eq!(
            validator.validate(&upstream).await,
            UpstreamAvailability::Syncing
        );
    }

    #[test]
    fn known_chain_ids() {
        assert_eq!(evm_chain_id(ChainRef::ChainEthereum.into()), Some(1));
        assert_eq!(evm_chain_id(ChainRef::ChainEthereumClassic.into()), Some(61));
        assert_eq!(
            evm_chain_id(ChainRef::ChainSepolia.into()),
            Some(11_155_111)
        );
        // No EVM chain id for Bitcoin — the check is skipped
        assert_eq!(evm_chain_id(ChainRef::ChainBitcoin.into()), None);
    }
}
