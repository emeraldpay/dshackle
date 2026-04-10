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

//! Ethereum head height tracking.
//!
//! Two strategies, chosen by transport:
//! - **HTTP** — polls `eth_blockNumber` every 10 seconds.
//! - **WebSocket** — subscribes to `newHeads` for near-instant updates,
//!   with an initial `eth_blockNumber` call so we don't wait for the next block.
//!   Re-subscribes automatically when the connection drops and reconnects.

use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::ethereum::ws::EthereumWsUpstream;
use crate::upstream::head::CurrentHeight;
use crate::upstream::traits::RpcUpstream;
use std::sync::Arc;
use std::time::Duration;

/// How often to poll `eth_blockNumber` for HTTP upstreams.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

// ─── HTTP: periodic polling ─────────────────────────────────────────────────

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

// ─── WebSocket: newHeads subscription ───────────────────────────────────────

/// Spawns a background task that tracks head height via `newHeads` subscription.
///
/// On each (re)connection it fetches the current block number immediately and
/// then processes subscription notifications until the channel closes
/// (indicating a disconnect), at which point it retries.
pub fn start_ws_head(upstream: Arc<EthereumWsUpstream>) {
    let upstream_id = upstream.id().to_string();
    let height = upstream.head_height();

    tokio::spawn(async move {
        loop {
            // Fetch current height immediately so we don't wait for the next block
            poll_block_number(&upstream_id, upstream.as_ref(), &height).await;

            match upstream.subscribe("newHeads").await {
                Ok(mut rx) => {
                    tracing::debug!(upstream = %upstream_id, "listening for newHeads");
                    while let Some(payload) = rx.recv().await {
                        if let Some(h) = extract_block_number(payload.get()) {
                            tracing::trace!(upstream = %upstream_id, height = h, "newHeads block");
                            height.update(h);
                        }
                    }
                    // Channel closed — connection dropped, loop back to re-subscribe
                    tracing::debug!(upstream = %upstream_id, "newHeads subscription closed, will re-subscribe");
                }
                Err(e) => {
                    tracing::debug!(upstream = %upstream_id, error = %e, "newHeads subscribe failed, retrying");
                }
            }

            // Brief pause before retrying to avoid tight loops during reconnection
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

/// Extract the `number` field from a newHeads notification payload.
fn extract_block_number(raw_json: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(raw_json).ok()?;
    let hex = v.get("number")?.as_str()?;
    parse_hex_height(hex)
}

// ─── Shared hex parser ──────────────────────────────────────────────────────

/// Parse a hex string like `"0x10d4f"` or `0x10d4f` into a `u64`.
pub fn parse_hex_height(raw: &str) -> Option<u64> {
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

    #[test]
    fn extract_number_from_new_heads_payload() {
        let payload = r#"{"number":"0x1312d00","hash":"0xabc...","parentHash":"0xdef..."}"#;
        assert_eq!(extract_block_number(payload), Some(20_000_000));
    }

    #[test]
    fn extract_number_missing_field() {
        let payload = r#"{"hash":"0xabc..."}"#;
        assert_eq!(extract_block_number(payload), None);
    }
}
