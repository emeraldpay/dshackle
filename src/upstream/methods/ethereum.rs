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

//! Default Ethereum RPC method configuration.
//!
//! Defines which methods can be forwarded to an upstream and which are answered
//! with predefined responses (e.g. `net_version`, `eth_chainId`).
//!
//! Mirrors the legacy `DefaultEthereumMethods` Kotlin class.

use crate::blockchain::TargetBlockchain;
use crate::jsonrpc::RpcMethod;
use emerald_api::proto::common::ChainRef;
use serde_json::value::RawValue;
use std::collections::{HashMap, HashSet};

/// Ethereum method configuration for a specific chain.
///
/// Separates methods into two groups:
/// - **callable** — forwarded to the upstream node (e.g. `eth_getBalance`)
/// - **hardcoded** — answered locally with a static response (e.g. `net_version`)
pub struct EthereumMethods {
    callable: HashSet<RpcMethod>,
    hardcoded: HashMap<RpcMethod, Box<RawValue>>,
}

impl EthereumMethods {
    /// Build method configuration for the given chain.
    ///
    /// Chain-specific methods (`net_version`, `eth_chainId`) are only hardcoded
    /// for known chains; unknown chains let them pass through to the upstream.
    pub fn new(chain: TargetBlockchain) -> Self {
        Self {
            callable: build_callable_set(),
            hardcoded: build_hardcoded_map(chain),
        }
    }

    /// Consume self and return the two datasets:
    /// - callable methods (for [`MethodFilter`](super::MethodFilter))
    /// - hardcoded responses (for [`HardcodedMethods`](super::HardcodedMethods))
    pub fn into_parts(self) -> (HashSet<RpcMethod>, HashMap<RpcMethod, Box<RawValue>>) {
        (self.callable, self.hardcoded)
    }
}

// ─── Callable methods ──────────────────────────────────────────────────────

/// Methods that require only any valid response (no special quorum).
const ANY_RESPONSE_METHODS: &[&str] = &[
    "eth_gasPrice",
    "eth_call",
    "eth_estimateGas",
];

/// Methods that return data uniquely identified by hash (first valid response wins).
const FIRST_VALUE_METHODS: &[&str] = &[
    "eth_getBlockTransactionCountByHash",
    "eth_getUncleCountByBlockHash",
    "eth_getBlockByHash",
    "eth_getTransactionByHash",
    "eth_getTransactionByBlockHashAndIndex",
    "eth_getStorageAt",
    "eth_getCode",
    "eth_getUncleByBlockHashAndIndex",
    "eth_getLogs",
];

/// Methods that require special handling (nonce, broadcast, etc.).
const SPECIAL_METHODS: &[&str] = &[
    "eth_getTransactionCount",
    "eth_blockNumber",
    "eth_getBalance",
    "eth_sendRawTransaction",
];

/// Methods whose correctness depends on the upstream being at the right block height.
const HEAD_VERIFIED_METHODS: &[&str] = &[
    "eth_getBlockTransactionCountByNumber",
    "eth_getUncleCountByBlockNumber",
    "eth_getBlockByNumber",
    "eth_getTransactionByBlockNumberAndIndex",
    "eth_getTransactionReceipt",
    "eth_getUncleByBlockNumberAndIndex",
    "eth_feeHistory",
];

fn build_callable_set() -> HashSet<RpcMethod> {
    ANY_RESPONSE_METHODS
        .iter()
        .chain(FIRST_VALUE_METHODS)
        .chain(SPECIAL_METHODS)
        .chain(HEAD_VERIFIED_METHODS)
        .map(|s| RpcMethod::from(*s))
        .collect()
}

// ─── Hardcoded responses ───────────────────────────────────────────────────

/// Helper to insert a raw JSON value into the map.
fn insert_raw(map: &mut HashMap<RpcMethod, Box<RawValue>>, method: &str, json: &str) {
    map.insert(
        RpcMethod::from(method),
        RawValue::from_string(json.to_string()).expect("invalid hardcoded JSON"),
    );
}

fn build_hardcoded_map(chain: TargetBlockchain) -> HashMap<RpcMethod, Box<RawValue>> {
    let mut m = HashMap::new();

    // ── Chain-independent responses ──────────────────────────────────────

    insert_raw(&mut m, "net_peerCount", "\"0x2a\"");
    insert_raw(&mut m, "net_listening", "true");
    insert_raw(
        &mut m,
        "web3_clientVersion",
        &format!("\"EmeraldDshackle/{}\"", env!("CARGO_PKG_VERSION")),
    );
    insert_raw(&mut m, "eth_protocolVersion", "\"0x3f\"");
    insert_raw(&mut m, "eth_syncing", "false");
    insert_raw(
        &mut m,
        "eth_coinbase",
        "\"0x0000000000000000000000000000000000000000\"",
    );
    insert_raw(&mut m, "eth_mining", "false");
    insert_raw(&mut m, "eth_hashrate", "\"0x0\"");
    insert_raw(&mut m, "eth_accounts", "[]");

    // ── Chain-specific responses ────────────────────────────────────────

    if let Some(net_version) = net_version_for(chain) {
        insert_raw(&mut m, "net_version", &format!("\"{net_version}\""));
    }
    if let Some(chain_id) = chain_id_for(chain) {
        insert_raw(&mut m, "eth_chainId", &format!("\"{chain_id}\""));
    }

    m
}

fn net_version_for(chain: TargetBlockchain) -> Option<&'static str> {
    match chain {
        TargetBlockchain::Standard(c) => match c {
            ChainRef::ChainEthereum => Some("1"),
            ChainRef::ChainEthereumClassic => Some("1"),
            ChainRef::ChainMatic => Some("137"),
            ChainRef::ChainMorden => Some("2"),
            ChainRef::ChainRopsten => Some("3"),
            ChainRef::ChainRinkeby => Some("4"),
            ChainRef::ChainKovan => Some("42"),
            ChainRef::ChainGoerli => Some("5"),
            ChainRef::ChainHolesky => Some("17000"),
            ChainRef::ChainSepolia => Some("11155111"),
            ChainRef::ChainHoodi => Some("560048"),
            _ => None,
        },
    }
}

fn chain_id_for(chain: TargetBlockchain) -> Option<&'static str> {
    match chain {
        TargetBlockchain::Standard(c) => match c {
            ChainRef::ChainEthereum => Some("0x1"),
            ChainRef::ChainEthereumClassic => Some("0x3d"),
            ChainRef::ChainMatic => Some("0x89"),
            ChainRef::ChainMorden => Some("0x3c"),
            ChainRef::ChainRopsten => Some("0x3"),
            ChainRef::ChainRinkeby => Some("0x4"),
            ChainRef::ChainKovan => Some("0x2a"),
            ChainRef::ChainGoerli => Some("0x5"),
            ChainRef::ChainHolesky => Some("0x4268"),
            ChainRef::ChainSepolia => Some("0xaa36a7"),
            ChainRef::ChainHoodi => Some("0x88bb0"),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eth_chain() -> TargetBlockchain {
        TargetBlockchain::Standard(ChainRef::ChainEthereum)
    }

    fn sepolia_chain() -> TargetBlockchain {
        TargetBlockchain::Standard(ChainRef::ChainSepolia)
    }

    #[test]
    fn callable_includes_standard_methods() {
        let methods = EthereumMethods::new(eth_chain());
        let (callable, _) = methods.into_parts();

        assert!(callable.contains("eth_getBalance"));
        assert!(callable.contains("eth_blockNumber"));
        assert!(callable.contains("eth_getTransactionReceipt"));
        assert!(callable.contains("eth_sendRawTransaction"));
        assert!(callable.contains("eth_getLogs"));
    }

    #[test]
    fn callable_does_not_include_hardcoded() {
        let methods = EthereumMethods::new(eth_chain());
        let (callable, _) = methods.into_parts();

        assert!(!callable.contains("net_version"));
        assert!(!callable.contains("eth_chainId"));
        assert!(!callable.contains("web3_clientVersion"));
    }

    #[test]
    fn callable_does_not_include_unknown() {
        let methods = EthereumMethods::new(eth_chain());
        let (callable, _) = methods.into_parts();

        assert!(!callable.contains("debug_traceTransaction"));
        assert!(!callable.contains("eth_subscribe"));
    }

    #[test]
    fn hardcoded_ethereum_mainnet() {
        let methods = EthereumMethods::new(eth_chain());
        let (_, hardcoded) = methods.into_parts();

        assert_eq!(hardcoded["net_version"].get(), "\"1\"");
        assert_eq!(hardcoded["eth_chainId"].get(), "\"0x1\"");
        assert_eq!(hardcoded["eth_syncing"].get(), "false");
        assert_eq!(hardcoded["eth_accounts"].get(), "[]");
        assert_eq!(hardcoded["eth_mining"].get(), "false");
    }

    /// Data-driven test matching the legacy `DefaultEthereumMethodsSpec`
    /// "Provides hardcoded correct chainId" test across all supported chains.
    #[test]
    fn hardcoded_chain_id_for_all_chains() {
        let cases: Vec<(ChainRef, &str, &str)> = vec![
            (ChainRef::ChainEthereum, "\"0x1\"", "\"1\""),
            (ChainRef::ChainEthereumClassic, "\"0x3d\"", "\"1\""),
            (ChainRef::ChainMatic, "\"0x89\"", "\"137\""),
            (ChainRef::ChainKovan, "\"0x2a\"", "\"42\""),
            (ChainRef::ChainGoerli, "\"0x5\"", "\"5\""),
            (ChainRef::ChainRinkeby, "\"0x4\"", "\"4\""),
            (ChainRef::ChainRopsten, "\"0x3\"", "\"3\""),
            (ChainRef::ChainSepolia, "\"0xaa36a7\"", "\"11155111\""),
            (ChainRef::ChainHolesky, "\"0x4268\"", "\"17000\""),
            (ChainRef::ChainHoodi, "\"0x88bb0\"", "\"560048\""),
            (ChainRef::ChainMorden, "\"0x3c\"", "\"2\""),
        ];

        for (chain_ref, expected_chain_id, expected_net_version) in cases {
            let chain = TargetBlockchain::Standard(chain_ref);
            let methods = EthereumMethods::new(chain);
            let (_, hardcoded) = methods.into_parts();

            assert_eq!(
                hardcoded["eth_chainId"].get(),
                expected_chain_id,
                "eth_chainId mismatch for {:?}",
                chain_ref,
            );
            assert_eq!(
                hardcoded["net_version"].get(),
                expected_net_version,
                "net_version mismatch for {:?}",
                chain_ref,
            );
        }
    }

    #[test]
    fn hardcoded_version_string() {
        let methods = EthereumMethods::new(eth_chain());
        let (_, hardcoded) = methods.into_parts();

        let version = hardcoded["web3_clientVersion"].get();
        assert!(version.starts_with("\"EmeraldDshackle/"));
        assert!(version.ends_with('"'));
    }

    #[test]
    fn unknown_chain_omits_chain_specific_methods() {
        let chain = TargetBlockchain::Standard(ChainRef::ChainFantom);
        let methods = EthereumMethods::new(chain);
        let (_, hardcoded) = methods.into_parts();

        // Chain-independent methods still present
        assert!(hardcoded.contains_key("eth_syncing"));
        assert!(hardcoded.contains_key("web3_clientVersion"));

        // Chain-specific methods absent (will be forwarded to upstream)
        assert!(!hardcoded.contains_key("net_version"));
        assert!(!hardcoded.contains_key("eth_chainId"));
    }
}
