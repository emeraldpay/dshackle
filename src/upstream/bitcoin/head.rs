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

//! Bitcoin head height tracking via RPC polling.
//!
//! Unlike Ethereum, Bitcoin nodes don't offer a WebSocket `newHeads`
//! subscription. Instead we poll by calling `getbestblockhash` and, when the
//! hash changes, fetching the block with `getblock` to extract its height.
//!
//! The polling interval is 15 seconds, matching the legacy implementation.

use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::head::CurrentHeight;
use crate::upstream::traits::RpcUpstream;
use std::sync::Arc;
use std::time::Duration;

/// How often to poll for new blocks.
const POLL_INTERVAL: Duration = Duration::from_secs(15);

/// Spawns a background task that polls the Bitcoin node for the current best
/// block and updates the shared height tracker.
///
/// The flow mirrors the legacy `BitcoinRpcHead`:
/// 1. Call `getbestblockhash` to get the tip hash
/// 2. If the hash changed since last poll, call `getblock` to get full block info
/// 3. Extract the `height` field from the block response
pub fn start_head_poller(
    upstream_id: String,
    upstream: Arc<dyn RpcUpstream>,
    height: Arc<CurrentHeight>,
) {
    tokio::spawn(async move {
        let mut last_hash: Option<String> = None;
        let mut interval = tokio::time::interval(POLL_INTERVAL);

        loop {
            interval.tick().await;
            poll_best_block(&upstream_id, upstream.as_ref(), &height, &mut last_hash).await;
        }
    });
}

async fn poll_best_block(
    upstream_id: &str,
    upstream: &dyn RpcUpstream,
    height: &CurrentHeight,
    last_hash: &mut Option<String>,
) {
    // Step 1: get the best block hash
    let hash_request = JsonRpcRequest::new(0, "getbestblockhash".into(), serde_json::json!([]));
    let hash_str = match upstream.call(&hash_request).await {
        Ok(resp) => match &resp.result {
            Some(raw) => {
                let s = raw.get().trim().trim_matches('"').to_string();
                if s.is_empty() {
                    tracing::warn!(upstream = upstream_id, "empty getbestblockhash response");
                    return;
                }
                s
            }
            None => {
                tracing::debug!(upstream = upstream_id, "getbestblockhash returned no result");
                return;
            }
        },
        Err(e) => {
            tracing::debug!(upstream = upstream_id, error = %e, "getbestblockhash poll failed");
            return;
        }
    };

    // Only fetch block details when the hash changes (avoids redundant RPC calls)
    if last_hash.as_deref() == Some(&hash_str) {
        return;
    }
    *last_hash = Some(hash_str.clone());

    // Step 2: fetch the block to get its height
    // getblock with verbosity=1 returns JSON with full block info
    let block_request = JsonRpcRequest::new(
        0,
        "getblock".into(),
        serde_json::json!([hash_str, 1]),
    );
    match upstream.call(&block_request).await {
        Ok(resp) => {
            if let Some(raw) = &resp.result {
                match extract_block_height(raw.get()) {
                    Some(h) => {
                        tracing::trace!(upstream = upstream_id, height = h, hash = %last_hash.as_deref().unwrap_or(""), "head updated");
                        height.update(h);
                    }
                    None => {
                        tracing::warn!(
                            upstream = upstream_id,
                            "failed to extract height from getblock response"
                        );
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!(upstream = upstream_id, error = %e, "getblock call failed");
        }
    }
}

/// Extracts the `height` field from a `getblock` JSON response.
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
            "previousblockhash": "00000000000000000000deadbeef",
            "tx": ["abc123", "def456"]
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
}
