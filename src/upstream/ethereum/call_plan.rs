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

//! Ethereum [`CallPlanner`].
//!
//! Finds the block a call reads: the block parameter of a state read
//! (`eth_call`, `eth_getBalance`, ...) or of a block read by number
//! (`eth_getBlockByNumber`, ...), with `latest` resolved to the chain head.
//!
//! And, as the legacy `NormalizingReader` did, rewrites `eth_getBlockByNumber`
//! towards the immutable block-by-hash form:
//!
//! 1. A tag (`latest`, `earliest`) becomes a concrete height. `pending` has no
//!    block yet and is left as is.
//! 2. A height whose hash is in the height→hash cache becomes
//!    `eth_getBlockByHash`, which the per-upstream cache can answer. That
//!    cache only keeps recent heights, so this applies near the head, the
//!    window legacy limited it to explicitly.

use crate::cache::Caches;
use crate::data::BlockId;
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::call_plan::{BlockRead, CallPlan, CallPlanner};
use crate::upstream::ethereum::parse_hex_quantity;
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;

/// Length of a `0x`-prefixed 32-byte block hash, which is a valid block
/// parameter in place of a height.
const HASH_LEN: usize = 66;

/// Prepares Ethereum calls for routing (see the module doc): finds the block
/// each reads, and pins `eth_getBlockByNumber` to its block.
pub struct EthereumCallPlanner {
    caches: Arc<Caches>,
}

impl EthereumCallPlanner {
    /// `caches` are the chain's: its height→hash index resolves heights to
    /// hashes and back.
    pub fn new(caches: Arc<Caches>) -> Self {
        Self { caches }
    }

    /// The height of the block a state read is pinned to.
    fn state_height(&self, param: &Value, head: &dyn Fn() -> Option<u64>) -> Option<u64> {
        match param {
            Value::String(tag) => match tag.as_str() {
                "latest" => head(),
                "earliest" => Some(0),
                // `pending`, `safe`, `finalized`: nothing definite to check.
                tag if !tag.starts_with("0x") => None,
                hash if hash.len() == HASH_LEN => self.height_of_hash(hash, head),
                number => parse_hex_quantity(number),
            },
            // EIP-1898: `{"blockNumber": "0x..."}` or `{"blockHash": "0x..."}`.
            Value::Object(fields) => {
                if let Some(number) = fields.get("blockNumber").and_then(|v| v.as_str()) {
                    parse_hex_quantity(number)
                } else if let Some(hash) = fields.get("blockHash").and_then(|v| v.as_str()) {
                    self.height_of_hash(hash, head)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// A hash known to the cache has its height; any other is taken as the
    /// head, which is over-strict for an old block, but an upstream at the
    /// head has every earlier block.
    fn height_of_hash(&self, hash: &str, head: &dyn Fn() -> Option<u64>) -> Option<u64> {
        hash.parse::<BlockId>()
            .ok()
            .and_then(|id| self.caches.get_height_by_hash(&id))
            .or_else(head)
    }

    /// Rewrite an `eth_getBlockByNumber` for the block at `height` into its
    /// most definite form (see the module doc).
    fn pin<'a>(
        &self,
        request: &'a JsonRpcRequest,
        block_ref: &str,
        height: u64,
    ) -> Cow<'a, JsonRpcRequest> {
        if request.method.as_str() != "eth_getBlockByNumber" {
            return Cow::Borrowed(request);
        }
        if let Some(hash) = self.caches.get_hash_by_height(height) {
            return Cow::Owned(with_first_param(
                request,
                "eth_getBlockByHash",
                hash.to_hex_prefixed(),
            ));
        }
        if !block_ref.starts_with("0x") {
            // Not cacheable by height, but it pins the answer, so a retry on
            // another upstream reads the same block.
            return Cow::Owned(with_first_param(
                request,
                "eth_getBlockByNumber",
                format!("0x{height:x}"),
            ));
        }
        Cow::Borrowed(request)
    }
}

impl CallPlanner for EthereumCallPlanner {
    fn plan<'a>(
        &self,
        request: &'a JsonRpcRequest,
        head: &dyn Fn() -> Option<u64>,
    ) -> CallPlan<'a> {
        let method = request.method.as_str();
        let params = request.params.as_array();

        if let Some(pos) = state_param_index(method) {
            let block = params
                .and_then(|p| p.get(pos))
                .and_then(|param| self.state_height(param, head))
                .map(BlockRead::State);
            return CallPlan {
                request: Cow::Borrowed(request),
                block,
            };
        }

        let Some(pos) = block_param_index(method) else {
            return CallPlan::as_is(request);
        };
        let Some(block_ref) = params.and_then(|p| p.get(pos)).and_then(|v| v.as_str()) else {
            return CallPlan::as_is(request);
        };
        let Some(height) = block_height(block_ref, head) else {
            return CallPlan::as_is(request);
        };
        CallPlan {
            request: self.pin(request, block_ref, height),
            block: Some(BlockRead::Block(height)),
        }
    }
}

/// Position of the block parameter in methods that read state at a block.
fn state_param_index(method: &str) -> Option<usize> {
    match method {
        "eth_call" | "eth_getBalance" | "eth_getCode" | "eth_getTransactionCount" => Some(1),
        "eth_getStorageAt" => Some(2),
        _ => None,
    }
}

/// Position of the block parameter in methods that read a block by number.
fn block_param_index(method: &str) -> Option<usize> {
    match method {
        "eth_getBlockByNumber"
        | "eth_getTransactionByBlockNumberAndIndex"
        | "eth_getBlockTransactionCountByNumber"
        | "eth_getUncleCountByBlockNumber"
        | "eth_getUncleByBlockNumberAndIndex" => Some(0),
        "eth_feeHistory" => Some(1),
        _ => None,
    }
}

/// The height a block read by number refers to. `pending` and the other
/// tags, and anything malformed, are left for the upstream to interpret.
fn block_height(block_ref: &str, head: &dyn Fn() -> Option<u64>) -> Option<u64> {
    match block_ref {
        "latest" => head(),
        "earliest" => Some(0),
        number if number.starts_with("0x") && number.len() != HASH_LEN => {
            parse_hex_quantity(number)
        }
        _ => None,
    }
}

/// A copy of the request with the method and the first parameter replaced,
/// keeping the other parameters (e.g. the full-transactions flag) and the
/// signing nonce as the client sent them.
fn with_first_param(request: &JsonRpcRequest, method: &str, first: String) -> JsonRpcRequest {
    let mut params = vec![Value::String(first)];
    if let Some(rest) = request.params.as_array() {
        params.extend_from_slice(&rest[1..]);
    }
    let mut rewritten = JsonRpcRequest::new(request.id, method.into(), Value::Array(params));
    rewritten.nonce = request.nonce;
    rewritten
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CacheTag;
    use crate::data::BlockContainer;

    const HEAD: u64 = 1000;
    const HASH_HEX: &str = "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";

    fn caches_with_block(height: u64) -> Arc<Caches> {
        let caches = Caches::new();
        caches.cache(
            CacheTag::Latest,
            BlockContainer {
                hash: HASH_HEX.parse().unwrap(),
                height,
                parent_hash: None,
                total_difficulty: alloy::primitives::U256::ZERO,
                timestamp: jiff::Timestamp::UNIX_EPOCH,
                transaction_hashes: vec![],
                json: None,
                header_json: None,
            },
        );
        Arc::new(caches)
    }

    fn request(method: &str, params: Value) -> JsonRpcRequest {
        JsonRpcRequest::new(1, method.into(), params)
    }

    /// The block a call reads, with an empty cache and the head at [`HEAD`].
    fn block_of(method: &str, params: Value) -> Option<BlockRead> {
        EthereumCallPlanner::new(Arc::new(Caches::new()))
            .plan(&request(method, params), &|| Some(HEAD))
            .block
    }

    const ADDRESS: &str = "0x690b2bdf41f33f9f251ae0459e5898b856ed96be";

    // ── State reads ────────────────────────────────────────────────────

    #[test]
    fn state_at_latest_is_the_head() {
        let block = block_of("eth_getBalance", serde_json::json!([ADDRESS, "latest"]));
        assert_eq!(block, Some(BlockRead::State(HEAD)));
    }

    #[test]
    fn state_at_earliest_is_genesis() {
        let block = block_of("eth_getBalance", serde_json::json!([ADDRESS, "earliest"]));
        assert_eq!(block, Some(BlockRead::State(0)));
    }

    #[test]
    fn state_at_a_number() {
        let block = block_of("eth_call", serde_json::json!([{"to": "0x00"}, "0x64"]));
        assert_eq!(block, Some(BlockRead::State(100)));
        let block = block_of(
            "eth_getStorageAt",
            serde_json::json!([ADDRESS, "0x0", "0x2a"]),
        );
        assert_eq!(block, Some(BlockRead::State(42)));
        let block = block_of(
            "eth_call",
            serde_json::json!([{"to": "0x00"}, {"blockNumber": "0x64"}]),
        );
        assert_eq!(block, Some(BlockRead::State(100)));
    }

    #[test]
    fn state_at_an_unknown_hash_needs_the_head() {
        let hash = format!("0x{}", "ab".repeat(32));
        let block = block_of("eth_call", serde_json::json!([{"to": "0x00"}, hash]));
        assert_eq!(block, Some(BlockRead::State(HEAD)));
        let block = block_of(
            "eth_call",
            serde_json::json!([{"to": "0x00"}, {"blockHash": format!("0x{}", "cd".repeat(32))}]),
        );
        assert_eq!(block, Some(BlockRead::State(HEAD)));
    }

    #[test]
    fn state_at_a_cached_hash_is_its_height() {
        let planner = EthereumCallPlanner::new(caches_with_block(100));
        let req = request("eth_call", serde_json::json!([{"to": "0x00"}, HASH_HEX]));

        assert_eq!(
            planner.plan(&req, &|| Some(HEAD)).block,
            Some(BlockRead::State(100))
        );
    }

    #[test]
    fn state_without_a_definite_block_has_none() {
        for params in [
            serde_json::json!([ADDRESS, "pending"]),
            serde_json::json!([ADDRESS]),
            serde_json::json!([ADDRESS, "not-a-tag"]),
            serde_json::json!([ADDRESS, 42]),
            serde_json::json!([ADDRESS, {"something": "else"}]),
        ] {
            assert_eq!(block_of("eth_getBalance", params.clone()), None, "{params}");
        }
    }

    #[test]
    fn state_at_latest_without_a_head_has_none() {
        let planner = EthereumCallPlanner::new(Arc::new(Caches::new()));
        let req = request("eth_getBalance", serde_json::json!([ADDRESS, "latest"]));
        assert_eq!(planner.plan(&req, &|| None).block, None);
    }

    #[test]
    fn state_reads_are_not_rewritten() {
        let planner = EthereumCallPlanner::new(caches_with_block(HEAD));
        let req = request("eth_getBalance", serde_json::json!([ADDRESS, "latest"]));
        assert!(!planner.plan(&req, &|| Some(HEAD)).is_rewrite());
    }

    // ── Block reads ────────────────────────────────────────────────────

    #[test]
    fn block_reads_by_number() {
        for (method, params) in [
            ("eth_getBlockByNumber", serde_json::json!(["0x64", false])),
            (
                "eth_getTransactionByBlockNumberAndIndex",
                serde_json::json!(["0x64", "0x0"]),
            ),
            ("eth_feeHistory", serde_json::json!(["0x4", "0x64", []])),
        ] {
            assert_eq!(
                block_of(method, params),
                Some(BlockRead::Block(100)),
                "{method}"
            );
        }
    }

    #[test]
    fn block_read_at_latest_is_the_head() {
        let block = block_of("eth_getBlockByNumber", serde_json::json!(["latest", false]));
        assert_eq!(block, Some(BlockRead::Block(HEAD)));
    }

    #[test]
    fn block_read_without_a_definite_block_has_none() {
        for tag in ["pending", "finalized", "not_a_number", HASH_HEX] {
            assert_eq!(
                block_of("eth_getBlockByNumber", serde_json::json!([tag, false])),
                None,
                "{tag}"
            );
        }
        for params in [
            serde_json::json!([]),
            serde_json::json!([42, false]),
            serde_json::json!("latest"),
        ] {
            assert_eq!(
                block_of("eth_getBlockByNumber", params.clone()),
                None,
                "{params}"
            );
        }
    }

    #[test]
    fn head_is_only_looked_up_when_needed() {
        let planner = EthereumCallPlanner::new(Arc::new(Caches::new()));
        let unneeded = || -> Option<u64> { panic!("the head isn't needed for this call") };
        for (method, params) in [
            ("eth_getTransactionReceipt", serde_json::json!([HASH_HEX])),
            ("eth_getBlockByNumber", serde_json::json!(["0x64", false])),
            ("eth_getBalance", serde_json::json!([ADDRESS, "0x64"])),
        ] {
            planner.plan(&request(method, params), &unneeded);
        }
    }

    #[test]
    fn calls_without_a_block_parameter_have_none() {
        assert_eq!(
            block_of("eth_getTransactionReceipt", serde_json::json!([HASH_HEX])),
            None
        );
        assert_eq!(
            block_of("eth_getBlockByHash", serde_json::json!([HASH_HEX, false])),
            None
        );
    }

    // ── Rewriting eth_getBlockByNumber ─────────────────────────────────

    fn rewrite(params: Value, head: Option<u64>, caches: Arc<Caches>) -> Option<JsonRpcRequest> {
        let req = request("eth_getBlockByNumber", params);
        let plan = EthereumCallPlanner::new(caches).plan(&req, &|| head);
        plan.is_rewrite().then(|| plan.request.into_owned())
    }

    #[test]
    fn latest_with_cached_hash_becomes_by_hash() {
        let rewritten = rewrite(
            serde_json::json!(["latest", false]),
            Some(100),
            caches_with_block(100),
        )
        .unwrap();

        assert_eq!(rewritten.method.as_str(), "eth_getBlockByHash");
        assert_eq!(rewritten.params, serde_json::json!([HASH_HEX, false]));
        assert_eq!(rewritten.id, 1);
    }

    #[test]
    fn latest_without_cached_hash_becomes_the_head_height() {
        let rewritten = rewrite(
            serde_json::json!(["latest", true]),
            Some(0x10d4f),
            Arc::new(Caches::new()),
        )
        .unwrap();

        assert_eq!(rewritten.method.as_str(), "eth_getBlockByNumber");
        assert_eq!(rewritten.params, serde_json::json!(["0x10d4f", true]));
    }

    #[test]
    fn earliest_becomes_zero() {
        let rewritten = rewrite(
            serde_json::json!(["earliest", false]),
            None,
            Arc::new(Caches::new()),
        )
        .unwrap();

        assert_eq!(rewritten.params, serde_json::json!(["0x0", false]));
    }

    #[test]
    fn height_with_cached_hash_becomes_by_hash() {
        let rewritten = rewrite(
            serde_json::json!(["0x64", true]),
            None,
            caches_with_block(0x64),
        )
        .unwrap();

        assert_eq!(rewritten.method.as_str(), "eth_getBlockByHash");
        assert_eq!(rewritten.params, serde_json::json!([HASH_HEX, true]));
    }

    #[test]
    fn missing_full_flag_stays_missing() {
        let rewritten =
            rewrite(serde_json::json!(["0x64"]), None, caches_with_block(0x64)).unwrap();
        assert_eq!(rewritten.params, serde_json::json!([HASH_HEX]));
    }

    #[test]
    fn rewrite_keeps_the_signing_nonce() {
        let mut req = request("eth_getBlockByNumber", serde_json::json!(["0x64", false]));
        req.nonce = 42;
        let plan = EthereumCallPlanner::new(caches_with_block(0x64)).plan(&req, &|| None);

        assert_eq!(plan.request.nonce, 42);
    }

    #[test]
    fn not_rewritten_when_nothing_to_improve() {
        let caches = caches_with_block(HEAD);
        // A height with no cached hash, `pending`, a hash in place of a
        // height, `latest` with no head to resolve it to.
        for (params, head) in [
            (serde_json::json!(["0x64", false]), Some(HEAD)),
            (serde_json::json!(["pending", false]), Some(HEAD)),
            (serde_json::json!([HASH_HEX, false]), Some(HEAD)),
            (serde_json::json!(["latest", false]), None),
        ] {
            assert!(
                rewrite(params.clone(), head, Arc::clone(&caches)).is_none(),
                "{params}"
            );
        }
    }

    #[test]
    fn other_block_reads_are_not_rewritten() {
        let planner = EthereumCallPlanner::new(caches_with_block(100));
        let req = request(
            "eth_getTransactionByBlockNumberAndIndex",
            serde_json::json!(["0x64", "0x0"]),
        );
        assert!(!planner.plan(&req, &|| Some(HEAD)).is_rewrite());
    }
}
