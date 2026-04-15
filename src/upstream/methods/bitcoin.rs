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

//! Default Bitcoin RPC method configuration.
//!
//! Defines which methods can be forwarded to an upstream, which are answered
//! with predefined responses, and which `CallQuorum` strategy applies to
//! each. Mirrors the legacy `DefaultBitcoinMethods` Kotlin class.
//!
//! In the legacy code these decisions are configurable per blockchain and
//! per upstream. This is the chain-default state — per-upstream overrides
//! will plug in here later.

use crate::jsonrpc::RpcMethod;
use crate::upstream::quorum::{
    AlwaysQuorum, BroadcastQuorum, CallQuorum, NonEmptyQuorum, NotLaggingQuorum, QuorumFactory,
};
use serde_json::value::RawValue;
use std::collections::{HashMap, HashSet};

/// Bitcoin method configuration. Acts as both the source of allowed/hardcoded
/// method sets and the per-method [`QuorumFactory`].
///
/// TODO: this currently produces a fixed chain-default config. Rewrite to
/// accept per-blockchain and per-upstream overrides from the user's config
/// (allowed methods, hardcoded responses, quorum strategy per method).
pub struct BitcoinMethods {
    callable: HashSet<RpcMethod>,
    hardcoded: HashMap<RpcMethod, Box<RawValue>>,
}

impl BitcoinMethods {
    /// Build method configuration for Bitcoin.
    pub fn new() -> Self {
        Self {
            callable: build_callable_set(),
            hardcoded: build_hardcoded_map(),
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

/// Methods that need fresh data from the node.
const FRESH_METHODS: &[&str] = &[
    "getblock",
    "getmemorypool",
    "getrawmempool",
    "gettransaction",
    "gettxout",
];

/// Methods where any valid response is acceptable.
const ANY_RESPONSE_METHODS: &[&str] = &[
    "getblockhash",
    "getrawtransaction",
];

/// Methods verified against the current head.
const HEAD_VERIFIED_METHODS: &[&str] = &[
    "getbestblockhash",
    "getblocknumber",
    "getblockcount",
    "getreceivedbyaddress",
    "listunspent",
];

/// Methods for broadcasting transactions.
const BROADCAST_METHODS: &[&str] = &[
    "sendrawtransaction",
];

fn build_callable_set() -> HashSet<RpcMethod> {
    FRESH_METHODS
        .iter()
        .chain(ANY_RESPONSE_METHODS)
        .chain(HEAD_VERIFIED_METHODS)
        .chain(BROADCAST_METHODS)
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

// ─── Quorum strategy per method ────────────────────────────────────────────

/// Default per-method quorum mapping. Mirrors
/// `DefaultBitcoinMethods.createQuorumFor` from the legacy implementation.
impl QuorumFactory for BitcoinMethods {
    fn quorum_for(&self, method: &RpcMethod) -> Box<dyn CallQuorum> {
        match method.as_str() {
            // Head-verified reads require the upstream to be at the tip.
            "getbestblockhash"
            | "getblocknumber"
            | "getblockcount"
            | "getreceivedbyaddress"
            | "listunspent" => Box::new(NotLaggingQuorum::new(0)),
            // Fresh data: tolerate a couple of blocks of lag (Bitcoin blocks
            // are ~10 minutes apart, so the threshold maps to ~20 min).
            "getblock" | "getmemorypool" | "getrawmempool" | "gettransaction" | "gettxout" => {
                Box::new(NotLaggingQuorum::new(2))
            }
            // Some upstreams may not yet have the requested object — keep
            // polling until one does.
            "getblockhash" | "getrawtransaction" => Box::new(NonEmptyQuorum::new()),
            "sendrawtransaction" => Box::new(BroadcastQuorum::new()),
            _ => Box::new(AlwaysQuorum::new()),
        }
    }
}

// ─── Hardcoded responses ───────────────────────────────────────────────────

fn build_hardcoded_map() -> HashMap<RpcMethod, Box<RawValue>> {
    let mut m = HashMap::new();

    // Legacy returns "42" as a hardcoded peer count
    insert_raw(&mut m, "getconnectioncount", "42");

    // Network info with Dshackle version
    let network_info = format!(
        r#"{{"version":210100,"subversion":"/EmeraldDshackle:{}/"}}"#,
        env!("CARGO_PKG_VERSION")
    );
    insert_raw(&mut m, "getnetworkinfo", &network_info);

    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callable_includes_standard_methods() {
        let methods = BitcoinMethods::new();
        let (callable, _) = methods.into_parts();

        assert!(callable.contains("getblock"));
        assert!(callable.contains("getbestblockhash"));
        assert!(callable.contains("getblockcount"));
        assert!(callable.contains("sendrawtransaction"));
        assert!(callable.contains("getrawtransaction"));
        assert!(callable.contains("listunspent"));
        assert!(callable.contains("gettxout"));
    }

    #[test]
    fn callable_does_not_include_hardcoded() {
        let methods = BitcoinMethods::new();
        let (callable, _) = methods.into_parts();

        assert!(!callable.contains("getconnectioncount"));
        assert!(!callable.contains("getnetworkinfo"));
    }

    #[test]
    fn callable_does_not_include_unknown() {
        let methods = BitcoinMethods::new();
        let (callable, _) = methods.into_parts();

        assert!(!callable.contains("eth_getBalance"));
        assert!(!callable.contains("getwalletinfo"));
    }

    #[test]
    fn hardcoded_connection_count() {
        let methods = BitcoinMethods::new();
        let (_, hardcoded) = methods.into_parts();

        assert_eq!(hardcoded["getconnectioncount"].get(), "42");
    }

    #[test]
    fn hardcoded_network_info() {
        let methods = BitcoinMethods::new();
        let (_, hardcoded) = methods.into_parts();

        let info = hardcoded["getnetworkinfo"].get();
        assert!(info.contains("210100"), "should contain version");
        assert!(info.contains("EmeraldDshackle"), "should contain subversion");
    }

    // ── Quorum mapping ────────────────────────────────────────────────

    use crate::upstream::quorum::SelectorHint;

    fn selector_for(method: &str) -> SelectorHint {
        BitcoinMethods::new().quorum_for(&method.into()).selector()
    }

    #[test]
    fn send_raw_uses_broadcast_quorum() {
        // BroadcastQuorum reports `Available` as its selector.
        match selector_for("sendrawtransaction") {
            SelectorHint::Available => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn get_block_count_uses_strict_not_lagging() {
        match selector_for("getblockcount") {
            SelectorHint::NotLagging { max_lag: 0 } => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn get_block_tolerates_two_block_lag() {
        match selector_for("getblock") {
            SelectorHint::NotLagging { max_lag: 2 } => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_method_falls_back_to_always() {
        match selector_for("listwallets") {
            SelectorHint::Available => {}
            other => panic!("unexpected: {other:?}"),
        }
    }
}
