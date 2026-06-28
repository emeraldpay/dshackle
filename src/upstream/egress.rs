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

//! Egress (server-pushed) subscriptions.
//!
//! The topics behind the gRPC `NativeSubscribe` and the `eth_subscribe`
//! WebSocket proxy. Each chain exposes an [`EgressSubscription`] that turns a
//! topic + params into a stream of JSON messages. Ports the legacy
//! `EthereumEgressSubscription`.

use crate::data::BlockContainer;
use crate::upstream::merged_head::MergedHead;
use serde_json::json;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

/// `eth_subscribe` topics. `logs` (filtered) and `newPendingTransactions` are
/// added with later work.
pub const METHOD_NEW_HEADS: &str = "newHeads";
pub const METHOD_SYNCING: &str = "syncing";

/// Header fields a `newHeads` message carries beyond the parsed block metadata.
/// Passed through verbatim from the raw header JSON, matching the legacy
/// `NewHeadMessage` (which intentionally omits extraData, sha3Uncles, the state
/// / transactions / receipts roots, and the full transaction list).
const NEW_HEAD_HEADER_FIELDS: [&str; 6] = [
    "difficulty",
    "gasLimit",
    "gasUsed",
    "logsBloom",
    "miner",
    "baseFeePerGas",
];

/// How often the `syncing` topic re-checks the chain's status. The legacy
/// implementation is event-driven off `observeStatus`; the Rust upstreams
/// expose no status-change signal yet, so the egress polls and emits only on
/// change. The poll cadence is the one behavioral difference.
const SYNCING_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// A stream of JSON-encoded subscription messages.
pub type SubscriptionStream = Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>;

/// Why a subscription could not be started.
#[derive(Debug)]
pub enum EgressError {
    /// The topic isn't supported on this chain.
    UnsupportedMethod(String),
}

/// Server-pushed subscriptions for one chain.
pub trait EgressSubscription: Send + Sync {
    /// Subscribe to `method` with optional JSON `params`, returning a stream of
    /// JSON messages or an error when the topic isn't supported.
    fn subscribe(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<SubscriptionStream, EgressError>;
}

/// Source of the chain's aggregate syncing state, read by the `syncing` topic.
/// Implemented by `Multistream`; abstracted so the egress doesn't depend on the
/// whole routing layer (and stays trivially testable).
pub trait SyncingStatus: Send + Sync {
    /// `true` when the chain has no fully-synced upstream — the legacy
    /// `observeStatus() != OK`.
    fn is_syncing(&self) -> bool;
}

/// Ethereum egress: `newHeads` from the chain's merged head and `syncing` from
/// its aggregate status. Ports `EthereumEgressSubscription`.
pub struct EthereumEgress {
    head: Arc<MergedHead>,
    status: Arc<dyn SyncingStatus>,
}

impl EthereumEgress {
    pub fn new(head: Arc<MergedHead>, status: Arc<dyn SyncingStatus>) -> Self {
        Self { head, status }
    }
}

impl EgressSubscription for EthereumEgress {
    fn subscribe(
        &self,
        method: &str,
        _params: Option<serde_json::Value>,
    ) -> Result<SubscriptionStream, EgressError> {
        match method {
            METHOD_NEW_HEADS => {
                let stream = BroadcastStream::new(self.head.subscribe())
                    // Skip the gap markers the broadcast emits when a slow
                    // subscriber falls behind; only the latest head matters.
                    .filter_map(|item| item.ok().map(|block| new_head_message(&block)));
                Ok(Box::pin(stream))
            }
            METHOD_SYNCING => Ok(syncing_stream(Arc::clone(&self.status))),
            other => Err(EgressError::UnsupportedMethod(other.to_string())),
        }
    }
}

/// Build a `newHeads` message from a head block.
///
/// The core fields come from the parsed block metadata; the remaining header
/// fields are passed through verbatim from the raw header JSON (a `newHeads`
/// notification or, as a fallback, the full block JSON — both carry the same
/// header fields, already encoded as canonical hex by the node). When no raw
/// header is available the message degrades to the metadata-only subset rather
/// than guessing.
fn new_head_message(block: &BlockContainer) -> Vec<u8> {
    let mut msg = serde_json::Map::new();
    msg.insert("number".into(), json!(format!("0x{:x}", block.height)));
    msg.insert("hash".into(), json!(block.hash.to_hex_prefixed()));
    // Always present, matching the legacy `NewHeadMessage` (which never omits
    // the field); `null` only for heads that don't carry a parent, e.g. one
    // sourced from a remote Dshackle's `ChainHead`.
    msg.insert(
        "parentHash".into(),
        json!(block.parent_hash.map(|p| p.to_hex_prefixed())),
    );
    msg.insert(
        "timestamp".into(),
        json!(format!("0x{:x}", block.timestamp.as_second() as u64)),
    );

    let raw = block.header_json.as_deref().or(block.json.as_deref());
    if let Some(raw) = raw
        && let Ok(serde_json::Value::Object(header)) =
            serde_json::from_slice::<serde_json::Value>(raw)
    {
        for field in NEW_HEAD_HEADER_FIELDS {
            match header.get(field) {
                Some(value) if !value.is_null() => {
                    msg.insert(field.to_string(), value.clone());
                }
                _ => {}
            }
        }
    }

    serde_json::to_vec(&serde_json::Value::Object(msg)).expect("newHeads message always serializes")
}

/// Stream of `syncing` booleans: the current value immediately, then a new
/// value each time the chain's status flips. Polls on [`SYNCING_POLL_INTERVAL`]
/// while the value is unchanged.
fn syncing_stream(status: Arc<dyn SyncingStatus>) -> SubscriptionStream {
    let stream = futures::stream::unfold(
        (status, Option::<bool>::None),
        |(status, last)| async move {
            loop {
                let syncing = status.is_syncing();
                if last != Some(syncing) {
                    let msg = serde_json::to_vec(&serde_json::Value::Bool(syncing))
                        .expect("bool always serializes");
                    return Some((msg, (status, Some(syncing))));
                }
                tokio::time::sleep(SYNCING_POLL_INTERVAL).await;
            }
        },
    );
    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::BlockId;
    use crate::upstream::head::CurrentHead;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A full Ethereum header as a node delivers it over `newHeads`.
    const HEADER_JSON: &str = r#"{
        "number":"0x10","hash":"0xaa","parentHash":"0xbb","timestamp":"0x6553f100",
        "difficulty":"0x0","gasLimit":"0x1c9c380","gasUsed":"0x5208",
        "logsBloom":"0x00","miner":"0xcafe","baseFeePerGas":"0x7",
        "extraData":"0xdead","stateRoot":"0xfeed"
    }"#;

    fn block_with_header(height: u64, header: Option<&str>) -> BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = height as u8;
        BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: Some(BlockId::from_bytes([0xaa; 32])),
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::from_second(1_700_000_000).unwrap(),
            transaction_hashes: vec![],
            json: None,
            header_json: header.map(|h| Arc::from(h.as_bytes())),
        }
    }

    struct StubStatus(AtomicBool);
    impl StubStatus {
        fn new(syncing: bool) -> Arc<Self> {
            Arc::new(Self(AtomicBool::new(syncing)))
        }
        fn set(&self, syncing: bool) {
            self.0.store(syncing, Ordering::Relaxed);
        }
    }
    impl SyncingStatus for StubStatus {
        fn is_syncing(&self) -> bool {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn egress(head: Arc<MergedHead>, status: Arc<dyn SyncingStatus>) -> EthereumEgress {
        EthereumEgress::new(head, status)
    }

    #[test]
    fn new_head_message_carries_full_header_fields() {
        let msg = new_head_message(&block_with_header(0x10, Some(HEADER_JSON)));
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        // Core fields from metadata.
        assert_eq!(value["number"], "0x10");
        assert_eq!(value["timestamp"], "0x6553f100");
        assert!(value["hash"].as_str().unwrap().starts_with("0x"));
        assert!(value["parentHash"].as_str().unwrap().starts_with("0x"));
        // Header fields passed through from the raw header.
        assert_eq!(value["difficulty"], "0x0");
        assert_eq!(value["gasLimit"], "0x1c9c380");
        assert_eq!(value["gasUsed"], "0x5208");
        assert_eq!(value["logsBloom"], "0x00");
        assert_eq!(value["miner"], "0xcafe");
        assert_eq!(value["baseFeePerGas"], "0x7");
        // Intentionally-omitted fields stay out.
        assert!(value.get("extraData").is_none());
        assert!(value.get("stateRoot").is_none());
    }

    #[test]
    fn new_head_message_degrades_without_raw_header() {
        let msg = new_head_message(&block_with_header(0x10, None));
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert_eq!(value["number"], "0x10");
        assert!(value.get("difficulty").is_none());
        assert!(value.get("gasLimit").is_none());
    }

    #[test]
    fn new_head_message_keeps_null_parent_hash_key() {
        // A head without a parent (e.g. from a remote Dshackle ChainHead) still
        // carries the `parentHash` key, as `null` — never dropped.
        let mut block = block_with_header(0x10, None);
        block.parent_hash = None;
        let msg = new_head_message(&block);
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert!(value.as_object().unwrap().contains_key("parentHash"));
        assert!(value["parentHash"].is_null());
    }

    #[tokio::test]
    async fn new_heads_streams_head_messages() {
        let head = Arc::new(CurrentHead::new());
        let merged = MergedHead::new(vec![Arc::clone(&head)]);
        let egress = egress(merged, StubStatus::new(false));

        let mut stream = egress.subscribe(METHOD_NEW_HEADS, None).unwrap();
        head.update_with_block(block_with_header(0x20, Some(HEADER_JSON)));

        let msg = stream.next().await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert_eq!(value["number"], "0x20");
        assert_eq!(value["gasLimit"], "0x1c9c380");
    }

    #[tokio::test]
    async fn syncing_emits_initial_then_on_change() {
        let status = StubStatus::new(false);
        let merged = MergedHead::new(vec![]);
        let egress = egress(merged, Arc::clone(&status) as Arc<dyn SyncingStatus>);

        let mut stream = egress.subscribe(METHOD_SYNCING, None).unwrap();

        // Initial value is emitted immediately.
        let first: serde_json::Value = serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
        assert_eq!(first, serde_json::json!(false));

        // The change is observed on the next poll, which happens as soon as the
        // stream is polled again — no waiting on the interval.
        status.set(true);
        let second: serde_json::Value =
            serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
        assert_eq!(second, serde_json::json!(true));
    }

    #[tokio::test]
    async fn unsupported_method_errors() {
        let merged = MergedHead::new(vec![]);
        let egress = egress(merged, StubStatus::new(false));
        let err = egress
            .subscribe("logs", None)
            .err()
            .expect("logs is not supported yet");
        assert!(matches!(err, EgressError::UnsupportedMethod(m) if m == "logs"));
    }
}
