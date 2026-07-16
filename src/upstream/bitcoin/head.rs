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

//! Bitcoin head tracking via RPC polling.
//!
//! Unlike Ethereum, Bitcoin nodes don't offer a WebSocket `newHeads`
//! subscription. Instead we poll by calling `getbestblockhash` and, when the
//! hash changes, fetching the block with `getblock` to extract its data.
//!
//! The polling interval is 15 seconds, matching the legacy implementation.
//! Parsed blocks are pushed through [`CurrentHead::update_with_block`] so
//! downstream consumers (like [`CachingHead`](crate::cache::CachingHead))
//! receive the data.

use crate::data::{BlockContainer, BlockId, TxId};
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::head::CurrentHead;
use crate::upstream::traits::RpcUpstream;
use alloy::primitives::U256;
use std::sync::Arc;
use std::time::Duration;

/// How often to poll for new blocks.
const POLL_INTERVAL: Duration = Duration::from_secs(15);

/// Spawns a background task that polls the Bitcoin node for the current best
/// block and updates the shared head tracker.
///
/// The flow mirrors the legacy `BitcoinRpcHead`:
/// 1. Call `getbestblockhash` to get the tip hash
/// 2. If the hash changed since last poll, call `getblock` to get full block info
/// 3. Parse the block into a [`BlockContainer`] and push it to the head
pub fn start_head_poller(upstream: Arc<dyn RpcUpstream>, head: Arc<CurrentHead>) {
    tokio::spawn(async move {
        let mut last_hash: Option<String> = None;
        let mut interval = tokio::time::interval(POLL_INTERVAL);

        loop {
            interval.tick().await;
            poll_best_block(upstream.as_ref(), &head, &mut last_hash).await;
        }
    });
}

async fn poll_best_block(
    upstream: &dyn RpcUpstream,
    head: &CurrentHead,
    last_hash: &mut Option<String>,
) {
    let upstream_id = upstream.id();
    // Step 1: get the best block hash
    let hash_request = JsonRpcRequest::new(0, "getbestblockhash".into(), serde_json::json!([]));
    let hash_str = match upstream.call(&hash_request).await {
        Ok(resp) => match &resp.result {
            Some(raw) => {
                let s = raw.get().trim().trim_matches('"').to_string();
                if s.is_empty() {
                    tracing::warn!(upstream = %upstream_id, "empty getbestblockhash response");
                    return;
                }
                s
            }
            None => {
                tracing::debug!(
                    upstream = %upstream_id,
                    "getbestblockhash returned no result"
                );
                return;
            }
        },
        Err(e) => {
            tracing::debug!(upstream = %upstream_id, error = %e, "getbestblockhash poll failed");
            return;
        }
    };

    // Only fetch block details when the hash changes (avoids redundant RPC calls)
    if last_hash.as_deref() == Some(&hash_str) {
        return;
    }
    *last_hash = Some(hash_str.clone());

    // Step 2: fetch the block — verbosity=1 returns JSON with tx hashes
    let block_request = JsonRpcRequest::new(0, "getblock".into(), serde_json::json!([hash_str, 1]));
    match upstream.call(&block_request).await {
        Ok(resp) => {
            if let Some(raw) = &resp.result {
                match parse_btc_block(raw.get()) {
                    Some(block) => {
                        tracing::trace!(
                            upstream = %upstream_id,
                            height = block.height,
                            hash = %block.hash,
                            "head updated"
                        );
                        head.update_with_block(block);
                    }
                    None => {
                        // Fallback: try to extract at least the height
                        if let Some(h) = extract_block_height(raw.get()) {
                            tracing::debug!(
                                upstream = %upstream_id,
                                height = h,
                                "partial head update (block parse failed)"
                            );
                            head.update(h);
                        } else {
                            tracing::warn!(
                                upstream = %upstream_id,
                                "failed to parse getblock response"
                            );
                        }
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!(upstream = %upstream_id, error = %e, "getblock call failed");
        }
    }
}

// ─── Block parsing ──────────────────────────────────────────────────────────

/// Parse a Bitcoin `getblock` (verbosity=1) JSON response into a
/// [`BlockContainer`].
///
/// Bitcoin block JSON uses plain integers for height and time (unix seconds),
/// and hex strings without `0x` prefix for hashes.
pub(crate) fn parse_btc_block(raw_json: &str) -> Option<BlockContainer> {
    parse_btc_block_value(raw_json, &serde_json::from_str(raw_json).ok()?)
}

/// [`parse_btc_block`] from an already-decoded value, so a caller that has
/// parsed the JSON to return it doesn't parse it a second time to cache it.
/// `raw_json` is the same bytes `v` was decoded from, retained verbatim as the
/// block's stored JSON.
pub(crate) fn parse_btc_block_value(
    raw_json: &str,
    v: &serde_json::Value,
) -> Option<BlockContainer> {
    let hash: BlockId = v.get("hash")?.as_str()?.parse().ok()?;
    let height = v.get("height")?.as_u64()?;
    let parent_hash = v
        .get("previousblockhash")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok());
    let time = v.get("time")?.as_u64()?;
    let timestamp = jiff::Timestamp::from_second(time as i64).ok()?;
    // `chainwork` is the cumulative proof-of-work as a 256-bit hex string.
    let total_difficulty = v
        .get("chainwork")
        .and_then(|w| w.as_str())
        .and_then(|s| {
            let hex = s
                .strip_prefix("0x")
                .or_else(|| s.strip_prefix("0X"))
                .unwrap_or(s);
            U256::from_str_radix(hex, 16).ok()
        })
        .unwrap_or(U256::ZERO);

    let transaction_hashes = v
        .get("tx")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .filter_map(|s| s.parse::<TxId>().ok())
                .collect()
        })
        .unwrap_or_default();

    Some(BlockContainer {
        hash,
        height,
        parent_hash,
        total_difficulty,
        timestamp,
        transaction_hashes,
        json: Some(Arc::from(raw_json.as_bytes())),
        // Bitcoin has no Ethereum-style newHeads header notification.
        header_json: None,
    })
}

/// Extracts only the `height` field from a `getblock` response (fallback).
fn extract_block_height(raw_json: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(raw_json).ok()?;
    v.get("height")?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_height_from_block() {
        let block = r#"{"hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","height":0,"version":1,"time":1231006505}"#;
        assert_eq!(extract_block_height(block), Some(0));
    }

    #[test]
    fn extract_height_from_typical_block() {
        let block = r#"{
            "hash": "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6",
            "confirmations": 1,
            "height": 800000,
            "version": 536870912,
            "time": 1690000000,
            "previousblockhash": "00000000000000000001deadbeef00000000000000000000000000000000beef",
            "tx": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
        }"#;
        assert_eq!(extract_block_height(block), Some(800_000));
    }

    #[test]
    fn extract_height_missing_field() {
        let block = r#"{"hash":"abc","version":1}"#;
        assert_eq!(extract_block_height(block), None);
    }

    #[test]
    fn extract_height_invalid_json() {
        assert_eq!(extract_block_height("not json"), None);
    }

    #[test]
    fn extract_height_null_height() {
        let block = r#"{"height":null}"#;
        assert_eq!(extract_block_height(block), None);
    }

    #[test]
    fn parse_full_btc_block() {
        let json = r#"{
            "hash": "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6",
            "height": 800000,
            "time": 1690000000,
            "previousblockhash": "00000000000000000001deadbeef00000000000000000000000000000000beef",
            "tx": [
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ],
            "confirmations": 1,
            "version": 536870912
        }"#;

        let block = parse_btc_block(json).unwrap();
        assert_eq!(block.height, 800_000);
        assert!(block.parent_hash.is_some());
        assert_eq!(block.transaction_hashes.len(), 2);
        assert!(block.json.is_some());
    }

    #[test]
    fn parse_btc_genesis_block() {
        // Genesis block has no previousblockhash
        let json = r#"{
            "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
            "height": 0,
            "time": 1231006505
        }"#;

        let block = parse_btc_block(json).unwrap();
        assert_eq!(block.height, 0);
        assert!(block.parent_hash.is_none());
        assert!(block.transaction_hashes.is_empty());
    }

    #[test]
    fn parse_btc_block_rejects_invalid() {
        assert!(parse_btc_block("not json").is_none());
        assert!(parse_btc_block(r#"{"height": 1}"#).is_none()); // missing hash
    }
}
