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

//! Ethereum-specific [`RequestNormalizer`] implementation.
//!
//! Rewrites `eth_getBlockByNumber` requests towards the immutable
//! block-by-hash form, in up to two steps:
//!
//! 1. A block tag (`latest` → current head height, `earliest` → `0`) is
//!    resolved to a concrete height. `pending` has no block yet and is left
//!    untouched.
//! 2. A concrete height is resolved to the block hash through the
//!    height→hash cache, turning the request into `eth_getBlockByHash`.
//!
//! When only step 1 succeeds (the hash is not cached) the request is still
//! rewritten to the concrete height: it is not cacheable, but it pins the
//! answer so retries on other upstreams return the same block. The
//! height→hash cache is a small LRU of recent heights, so step 2 naturally
//! applies only to blocks near the head — the same window the legacy
//! `NormalizingReader` enforced explicitly.

use crate::cache::normalizing_upstream::RequestNormalizer;
use crate::cache::Caches;
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::ethereum::parse_hex_quantity;
use crate::upstream::head::Head;

/// Ethereum request normalizer for block-by-number lookups.
pub struct EthereumNormalizer;

impl RequestNormalizer for EthereumNormalizer {
    fn normalize(
        &self,
        request: &JsonRpcRequest,
        head: &dyn Head,
        caches: &Caches,
    ) -> Option<JsonRpcRequest> {
        if request.method.as_str() != "eth_getBlockByNumber" {
            return None;
        }
        let params = request.params.as_array()?;
        let block_ref = params.first()?.as_str()?;

        let is_tag = matches!(block_ref, "latest" | "earliest");
        let height = match block_ref {
            // Without a known head there is nothing to resolve "latest" to
            "latest" => head.current_height()?,
            "earliest" => 0,
            // "pending" has no block hash by definition; any other value must
            // be a hex quantity — anything unparseable (including a full
            // 32-byte hash, which overflows u64) is left for the upstream to
            // reject with a proper error
            "pending" => return None,
            hex => parse_hex_quantity(hex)?,
        };

        if let Some(hash) = caches.get_hash_by_height(height) {
            return Some(with_first_param(
                request,
                "eth_getBlockByHash",
                hash.to_hex_prefixed(),
            ));
        }
        if is_tag {
            return Some(with_first_param(
                request,
                "eth_getBlockByNumber",
                format!("0x{height:x}"),
            ));
        }
        // Already a concrete height and the hash is unknown — nothing to improve
        None
    }
}

/// Build a copy of the request with the given method and the first parameter
/// replaced, keeping the remaining parameters (e.g. the full-transactions
/// flag) as the client sent them.
fn with_first_param(request: &JsonRpcRequest, method: &str, first: String) -> JsonRpcRequest {
    let mut params = vec![serde_json::Value::String(first)];
    if let Some(rest) = request.params.as_array() {
        params.extend_from_slice(&rest[1..]);
    }
    JsonRpcRequest::new(request.id, method.into(), serde_json::Value::Array(params))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CacheTag;
    use crate::data::{BlockContainer, BlockId};
    use crate::upstream::head::{CurrentHead, NoHead};

    const HASH_HEX: &str = "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";

    fn caches_with_block(height: u64) -> Caches {
        let caches = Caches::new();
        caches.cache(
            CacheTag::Latest,
            BlockContainer {
                hash: HASH_HEX.parse().unwrap(),
                height,
                parent_hash: None,
                timestamp: jiff::Timestamp::UNIX_EPOCH,
                transaction_hashes: vec![],
                json: None,
            },
        );
        caches
    }

    fn head_at(height: u64) -> CurrentHead {
        let head = CurrentHead::new();
        head.update(height);
        head
    }

    fn request(params: serde_json::Value) -> JsonRpcRequest {
        JsonRpcRequest::new(1, "eth_getBlockByNumber".into(), params)
    }

    #[test]
    fn latest_with_cached_hash_becomes_by_hash() {
        let normalizer = EthereumNormalizer;
        let req = request(serde_json::json!(["latest", false]));

        let normalized = normalizer
            .normalize(&req, &head_at(100), &caches_with_block(100))
            .unwrap();

        assert_eq!(normalized.method.as_str(), "eth_getBlockByHash");
        assert_eq!(normalized.params, serde_json::json!([HASH_HEX, false]));
        assert_eq!(normalized.id, 1);
    }

    #[test]
    fn latest_without_cached_hash_becomes_concrete_height() {
        let normalizer = EthereumNormalizer;
        let req = request(serde_json::json!(["latest", true]));

        let normalized = normalizer
            .normalize(&req, &head_at(0x10d4f), &Caches::new())
            .unwrap();

        assert_eq!(normalized.method.as_str(), "eth_getBlockByNumber");
        assert_eq!(normalized.params, serde_json::json!(["0x10d4f", true]));
    }

    #[test]
    fn latest_without_head_passes_through() {
        let normalizer = EthereumNormalizer;
        let req = request(serde_json::json!(["latest", false]));

        assert!(normalizer
            .normalize(&req, &NoHead, &caches_with_block(100))
            .is_none());
    }

    #[test]
    fn earliest_resolves_to_zero() {
        let normalizer = EthereumNormalizer;
        let req = request(serde_json::json!(["earliest", false]));

        let normalized = normalizer.normalize(&req, &NoHead, &Caches::new()).unwrap();

        assert_eq!(normalized.method.as_str(), "eth_getBlockByNumber");
        assert_eq!(normalized.params, serde_json::json!(["0x0", false]));
    }

    #[test]
    fn pending_passes_through() {
        let normalizer = EthereumNormalizer;
        let req = request(serde_json::json!(["pending", false]));

        assert!(normalizer
            .normalize(&req, &head_at(100), &caches_with_block(100))
            .is_none());
    }

    #[test]
    fn height_with_cached_hash_becomes_by_hash() {
        let normalizer = EthereumNormalizer;
        let req = request(serde_json::json!(["0x64", true]));

        let normalized = normalizer
            .normalize(&req, &NoHead, &caches_with_block(0x64))
            .unwrap();

        assert_eq!(normalized.method.as_str(), "eth_getBlockByHash");
        assert_eq!(normalized.params, serde_json::json!([HASH_HEX, true]));
    }

    #[test]
    fn height_without_cached_hash_passes_through() {
        let normalizer = EthereumNormalizer;
        let req = request(serde_json::json!(["0x64", false]));

        assert!(normalizer.normalize(&req, &NoHead, &Caches::new()).is_none());
    }

    #[test]
    fn preserves_missing_full_flag() {
        let normalizer = EthereumNormalizer;
        let req = request(serde_json::json!(["0x64"]));

        let normalized = normalizer
            .normalize(&req, &NoHead, &caches_with_block(0x64))
            .unwrap();

        assert_eq!(normalized.params, serde_json::json!([HASH_HEX]));
    }

    #[test]
    fn other_methods_pass_through() {
        let normalizer = EthereumNormalizer;
        let req = JsonRpcRequest::new(
            1,
            "eth_getBlockByHash".into(),
            serde_json::json!([HASH_HEX, false]),
        );

        assert!(normalizer
            .normalize(&req, &head_at(100), &caches_with_block(100))
            .is_none());
    }

    #[test]
    fn block_hash_as_first_param_passes_through() {
        // A 32-byte hash is not a valid height; the upstream should reject it
        let normalizer = EthereumNormalizer;
        let req = request(serde_json::json!([HASH_HEX, false]));

        assert!(normalizer
            .normalize(&req, &head_at(100), &caches_with_block(100))
            .is_none());
    }

    #[test]
    fn malformed_params_pass_through() {
        let normalizer = EthereumNormalizer;
        let head = head_at(100);
        let caches = caches_with_block(100);

        for params in [
            serde_json::json!([]),
            serde_json::json!([42, false]),
            serde_json::json!("latest"),
            serde_json::json!(["not_a_number", false]),
        ] {
            assert!(
                normalizer.normalize(&request(params.clone()), &head, &caches).is_none(),
                "params {params} must pass through"
            );
        }
    }
}
