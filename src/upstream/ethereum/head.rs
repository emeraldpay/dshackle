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

//! Ethereum head tracking.
//!
//! Two strategies, chosen by transport:
//! - **HTTP** — polls `eth_getBlockByNumber("latest")` every 10 seconds.
//! - **WebSocket** — subscribes to `newHeads` for near-instant updates,
//!   with an initial full-block fetch so we don't wait for the next block.
//!   Re-subscribes automatically when the connection drops and reconnects.
//!
//! Both paths parse the block into a [`BlockContainer`] and push it through
//! [`CurrentHead::update_with_block`] so downstream consumers (like
//! [`CachingHead`](crate::cache::CachingHead)) receive the data.

use crate::data::{BlockContainer, BlockId, TxId};
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::head::CurrentHead;
use crate::upstream::traits::RpcUpstream;
use std::sync::Arc;
use std::time::Duration;

use super::{parse_hex_quantity, EthereumWsUpstream};

/// How often to poll for HTTP upstreams.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

// ─── HTTP: periodic polling ─────────────────────────────────────────────────

/// Spawns a background task that polls `eth_getBlockByNumber("latest", false)`
/// on the given upstream and updates the shared head tracker with full block
/// data.
pub fn start_head_poller(
    upstream_id: String,
    upstream: Arc<dyn RpcUpstream>,
    head: Arc<CurrentHead>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POLL_INTERVAL);
        loop {
            interval.tick().await;
            poll_latest_block(&upstream_id, upstream.as_ref(), &head).await;
        }
    });
}

/// Fetches the latest block (with tx hashes, not full tx bodies) and pushes
/// it through the head tracker.
async fn poll_latest_block(
    upstream_id: &str,
    upstream: &dyn RpcUpstream,
    head: &CurrentHead,
) {
    let request = JsonRpcRequest::new(
        0,
        "eth_getBlockByNumber".into(),
        serde_json::json!(["latest", false]),
    );
    match upstream.call(&request).await {
        Ok(resp) => {
            if let Some(raw) = &resp.result {
                match parse_eth_block(raw.get()) {
                    Some(block) => {
                        tracing::trace!(
                            upstream = upstream_id,
                            height = block.height,
                            hash = %block.hash,
                            "head updated"
                        );
                        head.update_with_block(block);
                    }
                    None => {
                        // Fallback: try to extract at least the height
                        if let Some(h) = extract_block_number(raw.get()) {
                            tracing::debug!(
                                upstream = upstream_id,
                                height = h,
                                "partial head update (block parse failed)"
                            );
                            head.update(h);
                        } else {
                            tracing::warn!(
                                upstream = upstream_id,
                                "failed to parse eth_getBlockByNumber response"
                            );
                        }
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!(upstream = upstream_id, error = %e, "eth_getBlockByNumber poll failed");
        }
    }
}

// ─── WebSocket: newHeads subscription ───────────────────────────────────────

/// Spawns a background task that tracks head via `newHeads` subscription.
///
/// On each (re)connection it fetches the current block immediately and
/// then processes subscription notifications until the channel closes
/// (indicating a disconnect), at which point it retries.
pub fn start_ws_head(upstream: Arc<EthereumWsUpstream>) {
    let upstream_id = upstream.id().to_string();
    let head = upstream.head_height();

    tokio::spawn(async move {
        loop {
            // Fetch the full latest block immediately so we don't wait for the next one
            poll_latest_block(&upstream_id, upstream.as_ref(), &head).await;

            match upstream.subscribe("newHeads").await {
                Ok(mut rx) => {
                    tracing::debug!(upstream = %upstream_id, "listening for newHeads");
                    while let Some(payload) = rx.recv().await {
                        // newHeads gives us the header (hash, number, parentHash,
                        // timestamp) but NOT transaction hashes — that's fine,
                        // the CachingHead still gets the hash↔height mapping.
                        match parse_eth_head_notification(payload.get()) {
                            Some(block) => {
                                tracing::trace!(
                                    upstream = %upstream_id,
                                    height = block.height,
                                    hash = %block.hash,
                                    "newHeads block"
                                );
                                head.update_with_block(block);
                            }
                            None => {
                                // Fallback: extract just the height
                                if let Some(h) = extract_block_number(payload.get()) {
                                    tracing::trace!(
                                        upstream = %upstream_id,
                                        height = h,
                                        "newHeads height-only update"
                                    );
                                    head.update(h);
                                }
                            }
                        }
                    }
                    tracing::debug!(upstream = %upstream_id, "newHeads subscription closed, will re-subscribe");
                }
                Err(e) => {
                    tracing::debug!(upstream = %upstream_id, error = %e, "newHeads subscribe failed, retrying");
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

// ─── Block parsing ──────────────────────────────────────────────────────────

/// Parse a full Ethereum block JSON (from `eth_getBlockByNumber` with
/// `false` for transaction details). Includes transaction hashes and stores
/// the raw JSON for later re-serving.
fn parse_eth_block(raw_json: &str) -> Option<BlockContainer> {
    let v: serde_json::Value = serde_json::from_str(raw_json).ok()?;

    let hash: BlockId = v.get("hash")?.as_str()?.parse().ok()?;
    let height = parse_hex_quantity(v.get("number")?.as_str()?)?;
    let parent_hash = v
        .get("parentHash")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok());
    let timestamp_secs = parse_hex_quantity(v.get("timestamp")?.as_str()?)?;
    let timestamp = jiff::Timestamp::from_second(timestamp_secs as i64).ok()?;

    let transaction_hashes = v
        .get("transactions")
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
        timestamp,
        transaction_hashes,
        json: Some(Arc::from(raw_json.as_bytes())),
    })
}

/// Parse an Ethereum `newHeads` notification into a [`BlockContainer`].
///
/// The notification contains the block header but NOT the transaction list,
/// so `transaction_hashes` will be empty and `json` is `None` (incomplete
/// representation that shouldn't be served to callers).
fn parse_eth_head_notification(raw_json: &str) -> Option<BlockContainer> {
    let v: serde_json::Value = serde_json::from_str(raw_json).ok()?;

    let hash: BlockId = v.get("hash")?.as_str()?.parse().ok()?;
    let height = parse_hex_quantity(v.get("number")?.as_str()?)?;
    let parent_hash = v
        .get("parentHash")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok());
    let timestamp_secs = parse_hex_quantity(v.get("timestamp")?.as_str()?)?;
    let timestamp = jiff::Timestamp::from_second(timestamp_secs as i64).ok()?;

    Some(BlockContainer {
        hash,
        height,
        parent_hash,
        timestamp,
        transaction_hashes: vec![],
        json: None,
    })
}

/// Extract the `number` field from a block or newHeads JSON payload.
fn extract_block_number(raw_json: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(raw_json).ok()?;
    let hex = v.get("number")?.as_str()?;
    parse_hex_quantity(hex)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn parse_full_eth_block() {
        let json = r#"{
            "hash": "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75",
            "number": "0xc5043f",
            "parentHash": "0x787899711b862b77df8d2faa69de664048598265a9f96abf178d341076e200e0",
            "timestamp": "0x65a8b44c",
            "transactions": [
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ],
            "gasUsed": "0x1234"
        }"#;

        let block = parse_eth_block(json).unwrap();
        assert_eq!(block.height, 0xc5043f);
        assert_eq!(
            block.hash.to_hex(),
            "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75"
        );
        assert!(block.parent_hash.is_some());
        assert_eq!(block.transaction_hashes.len(), 2);
        assert!(block.json.is_some());
    }

    #[test]
    fn parse_eth_block_no_transactions() {
        let json = r#"{
            "hash": "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75",
            "number": "0x1",
            "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "timestamp": "0x0"
        }"#;

        let block = parse_eth_block(json).unwrap();
        assert_eq!(block.height, 1);
        assert!(block.transaction_hashes.is_empty());
    }

    #[test]
    fn parse_new_heads_notification() {
        let json = r#"{
            "hash": "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75",
            "number": "0x1312d00",
            "parentHash": "0x787899711b862b77df8d2faa69de664048598265a9f96abf178d341076e200e0",
            "timestamp": "0x65a8b44c"
        }"#;

        let block = parse_eth_head_notification(json).unwrap();
        assert_eq!(block.height, 20_000_000);
        assert!(block.transaction_hashes.is_empty());
        assert!(block.json.is_none());
    }

    #[test]
    fn parse_eth_block_rejects_invalid() {
        assert!(parse_eth_block("not json").is_none());
        assert!(parse_eth_block(r#"{"number": "0x1"}"#).is_none()); // missing hash
    }
}
