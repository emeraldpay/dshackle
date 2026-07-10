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

//! Derives a minimum-height routing constraint from an Ethereum state-read
//! request, so a call pinned to a block is only sent to upstreams that have
//! reached it. Port of the legacy `EthereumCallSelector`.
//!
//! A pruned or lagging node answers `eth_call`/`eth_getBalance` at a block it
//! doesn't have with an empty result instead of an error, so routing there
//! silently corrupts the response — the constraint has to be applied before
//! the call goes out.

use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::ethereum::parse_hex_quantity;

/// Position of the block-tag parameter for the given method, or `None` when
/// the method carries no block tag. `eth_getStorageAt` is the odd one out
/// with the tag at index 2 (after address and storage slot).
fn block_param_index(method: &str) -> Option<usize> {
    match method {
        "eth_call" | "eth_getBalance" | "eth_getCode" | "eth_getTransactionCount" => Some(1),
        "eth_getStorageAt" => Some(2),
        _ => None,
    }
}

/// The minimum head height an upstream must have reached to serve `request`,
/// or `None` when the request carries no such constraint (either the method
/// has no block tag, the tag is absent, or it cannot be interpreted).
///
/// `current_height` is queried only when the tag requires it (`"latest"` or a
/// block hash), so the common no-tag path costs nothing.
pub fn min_height_for<F>(request: &JsonRpcRequest, current_height: F) -> Option<u64>
where
    F: FnOnce() -> Option<u64>,
{
    let pos = block_param_index(request.method.as_str())?;
    let tag = request.params.as_array()?.get(pos)?;
    match tag {
        serde_json::Value::String(s) => height_of_tag(s, current_height),
        // EIP-1898: `{"blockNumber": "0x..."}` or `{"blockHash": "0x..."}`.
        serde_json::Value::Object(fields) => {
            if let Some(number) = fields.get("blockNumber").and_then(|v| v.as_str()) {
                parse_hex_quantity(number)
            } else if fields.get("blockHash").is_some() {
                // A hash-pinned read requires the current head: legacy resolves
                // hash→height through the block cache, which doesn't exist here
                // yet. Over-strict for old blocks, but an upstream at the head
                // has every earlier block too.
                current_height()
            } else {
                None
            }
        }
        _ => None,
    }
}

fn height_of_tag<F>(tag: &str, current_height: F) -> Option<u64>
where
    F: FnOnce() -> Option<u64>,
{
    match tag {
        "latest" => current_height(),
        "earliest" => Some(0),
        // "pending" and other named tags carry no height constraint.
        _ if !tag.starts_with("0x") => None,
        // 0x + 64 hex digits is a block hash, anything shorter is a number.
        // Hash-pinned reads require the current head (see the EIP-1898 arm).
        _ if tag.len() == 66 => current_height(),
        _ => parse_hex_quantity(tag),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(method: &str, params: serde_json::Value) -> JsonRpcRequest {
        JsonRpcRequest::new(1, method.into(), params)
    }

    const HEAD: u64 = 1000;

    fn min_height(method: &str, params: serde_json::Value) -> Option<u64> {
        min_height_for(&request(method, params), || Some(HEAD))
    }

    #[test]
    fn latest_requires_current_head() {
        let params = serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "latest"]);
        assert_eq!(min_height("eth_getBalance", params), Some(HEAD));
    }

    #[test]
    fn earliest_requires_nothing() {
        let params = serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "earliest"]);
        assert_eq!(min_height("eth_getBalance", params), Some(0));
    }

    #[test]
    fn pending_has_no_constraint() {
        let params = serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "pending"]);
        assert_eq!(min_height("eth_getBalance", params), None);
    }

    #[test]
    fn hex_number_is_the_height() {
        let params = serde_json::json!([{"to": "0x00"}, "0x64"]);
        assert_eq!(min_height("eth_call", params), Some(100));
    }

    #[test]
    fn block_hash_falls_back_to_current_head() {
        let hash = format!("0x{}", "ab".repeat(32));
        let params = serde_json::json!([{"to": "0x00"}, hash]);
        assert_eq!(min_height("eth_call", params), Some(HEAD));
    }

    #[test]
    fn storage_at_reads_third_param() {
        let params =
            serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "0x0", "0x2a"]);
        assert_eq!(min_height("eth_getStorageAt", params), Some(42));
    }

    #[test]
    fn eip1898_block_number() {
        let params = serde_json::json!([{"to": "0x00"}, {"blockNumber": "0x64"}]);
        assert_eq!(min_height("eth_call", params), Some(100));
    }

    #[test]
    fn eip1898_block_hash() {
        let params =
            serde_json::json!([{"to": "0x00"}, {"blockHash": format!("0x{}", "cd".repeat(32))}]);
        assert_eq!(min_height("eth_call", params), Some(HEAD));
    }

    #[test]
    fn eip1898_unknown_keys_ignored() {
        let params = serde_json::json!([{"to": "0x00"}, {"something": "else"}]);
        assert_eq!(min_height("eth_call", params), None);
    }

    #[test]
    fn missing_tag_means_no_constraint() {
        let params = serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be"]);
        assert_eq!(min_height("eth_getBalance", params), None);
    }

    #[test]
    fn other_methods_have_no_constraint() {
        let params = serde_json::json!(["0x64", true]);
        assert_eq!(min_height("eth_getBlockByNumber", params), None);
    }

    #[test]
    fn garbage_tag_means_no_constraint() {
        let params = serde_json::json!([{"to": "0x00"}, "not-a-tag"]);
        assert_eq!(min_height("eth_call", params), None);
        let params = serde_json::json!([{"to": "0x00"}, 42]);
        assert_eq!(min_height("eth_call", params), None);
    }

    #[test]
    fn latest_without_known_head_means_no_constraint() {
        let params = serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "latest"]);
        assert_eq!(
            min_height_for(&request("eth_getBalance", params), || None),
            None
        );
    }
}
