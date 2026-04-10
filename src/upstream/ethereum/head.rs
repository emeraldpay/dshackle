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

//! Ethereum head polling — periodically calls `eth_blockNumber` on an upstream
//! and updates a shared [`CurrentHeight`].
//!
//! Matches the legacy `EthereumRpcHead` which polls on a fixed interval.
//! A WebSocket subscription-based head (`newHeads`) can be added later.

use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::head::CurrentHeight;
use crate::upstream::traits::RpcUpstream;
use std::sync::Arc;
use std::time::Duration;

/// How often to poll `eth_blockNumber`.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Spawns a background task that polls `eth_blockNumber` on the given upstream
/// and updates the shared height tracker.
pub fn start_head_poller(
    upstream_id: String,
    upstream: Arc<dyn RpcUpstream>,
    height: Arc<CurrentHeight>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POLL_INTERVAL);
        loop {
            interval.tick().await;
            poll_block_number(&upstream_id, upstream.as_ref(), &height).await;
        }
    });
}

async fn poll_block_number(
    upstream_id: &str,
    upstream: &dyn RpcUpstream,
    height: &CurrentHeight,
) {
    let request = JsonRpcRequest::new(0, "eth_blockNumber".into(), serde_json::json!([]));
    match upstream.call(&request).await {
        Ok(resp) => {
            if let Some(raw) = &resp.result {
                match parse_hex_height(raw.get()) {
                    Some(h) => {
                        tracing::trace!(upstream = upstream_id, height = h, "head updated");
                        height.update(h);
                    }
                    None => {
                        tracing::warn!(
                            upstream = upstream_id,
                            raw = raw.get(),
                            "failed to parse eth_blockNumber response"
                        );
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!(upstream = upstream_id, error = %e, "eth_blockNumber poll failed");
        }
    }
}

/// Parse a JSON hex string like `"0x10d4f"` into a `u64`.
fn parse_hex_height(raw: &str) -> Option<u64> {
    // RawValue preserves JSON quotes: `"0x10d4f"`
    let s = raw.trim().trim_matches('"');
    let hex = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"))?;
    u64::from_str_radix(hex, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_typical_block_number() {
        assert_eq!(parse_hex_height(r#""0x10d4f""#), Some(0x10d4f));
    }

    #[test]
    fn parse_zero() {
        assert_eq!(parse_hex_height(r#""0x0""#), Some(0));
    }

    #[test]
    fn parse_large_block_number() {
        // Ethereum mainnet ~20M blocks
        assert_eq!(parse_hex_height(r#""0x1312d00""#), Some(20_000_000));
    }

    #[test]
    fn parse_without_quotes() {
        assert_eq!(parse_hex_height("0x10d4f"), Some(0x10d4f));
    }

    #[test]
    fn reject_non_hex() {
        assert_eq!(parse_hex_height(r#""not_hex""#), None);
    }

    #[test]
    fn reject_empty() {
        assert_eq!(parse_hex_height(r#""""#), None);
    }
}
