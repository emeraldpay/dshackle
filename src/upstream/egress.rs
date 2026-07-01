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

use crate::data::{BlockContainer, BlockId};
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::merged_head::MergedHead;
use crate::upstream::status_signal::StatusChanges;
use crate::upstream::traits::UpstreamError;
use serde_json::json;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

/// `eth_subscribe` topics. `newPendingTransactions` is deferred (needs a
/// pending-tx source).
pub const METHOD_NEW_HEADS: &str = "newHeads";
pub const METHOD_SYNCING: &str = "syncing";
pub const METHOD_LOGS: &str = "logs";

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

    /// Topics this egress can serve, for `Describe`'s `supportedSubscriptions`
    /// (legacy `EgressSubscription.getAvailableTopics`).
    fn available_topics(&self) -> Vec<String>;
}

/// What the egress needs from a chain's upstreams: the aggregate syncing state
/// (for `syncing`) and a way to make a call (for `logs`). Abstracted as a trait
/// so the egress doesn't depend on the whole routing layer and stays trivially
/// testable. Implemented by `Multistream`.
#[async_trait::async_trait]
pub trait ChainAccess: Send + Sync {
    /// `true` when the chain has no fully-synced upstream — the legacy
    /// `observeStatus() != OK`.
    fn is_syncing(&self) -> bool;

    /// The chain's current best height (max across upstreams), or `None` when no
    /// upstream has reported a head yet. Used by fee estimation to choose the
    /// block window (legacy `Multistream.getHead().getCurrentHeight()`). Defaults
    /// to `None` for consumers that don't track a head.
    fn current_height(&self) -> Option<u64> {
        None
    }

    /// Route a JSON-RPC request through the chain's upstreams (used to fetch
    /// `eth_getLogs` per head block).
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError>;

    /// Route a read of a specific block to an upstream that has actually reached
    /// `min_height`. A plain [`call`](Self::call) may pick an upstream lagging
    /// within the method's tolerance (e.g. `eth_getBlockByNumber` allows a
    /// one-block lag), which answers a tip read with `null` and can empty a
    /// small fee-sampling window. Defaults to `call` for accessors that don't
    /// track per-upstream heights.
    async fn call_at_height(
        &self,
        request: &JsonRpcRequest,
        _min_height: u64,
    ) -> Result<JsonRpcResponse, UpstreamError> {
        self.call(request).await
    }

    /// A subscription that wakes when the chain's aggregate status may have
    /// changed, so `syncing` reacts without polling. Defaults to a signal that
    /// never fires, for chain accessors that don't track upstream status.
    fn status_changes(&self) -> StatusChanges {
        StatusChanges::never()
    }
}

/// Ethereum egress: `newHeads` and `logs` from the chain's merged head and
/// `syncing` from its aggregate status. Ports `EthereumEgressSubscription`.
pub struct EthereumEgress {
    head: Arc<MergedHead>,
    access: Arc<dyn ChainAccess>,
}

impl EthereumEgress {
    pub fn new(head: Arc<MergedHead>, access: Arc<dyn ChainAccess>) -> Self {
        Self { head, access }
    }
}

impl EgressSubscription for EthereumEgress {
    fn subscribe(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<SubscriptionStream, EgressError> {
        match method {
            METHOD_NEW_HEADS => {
                let access = Arc::clone(&self.access);
                let stream = BroadcastStream::new(self.head.subscribe())
                    // Skip the gap markers the broadcast emits when a slow
                    // subscriber falls behind; only the latest head matters.
                    .filter_map(|item| item.ok())
                    .then(move |block| {
                        let access = Arc::clone(&access);
                        async move {
                            let raw = head_raw_header(&block, &*access).await;
                            new_head_message(&block, raw.as_deref())
                        }
                    });
                Ok(Box::pin(stream))
            }
            METHOD_SYNCING => Ok(syncing_stream(Arc::clone(&self.access))),
            METHOD_LOGS => Ok(logs_stream(
                Arc::clone(&self.head),
                Arc::clone(&self.access),
                LogsFilter::from_params(params),
            )),
            other => Err(EgressError::UnsupportedMethod(other.to_string())),
        }
    }

    fn available_topics(&self) -> Vec<String> {
        vec![
            METHOD_NEW_HEADS.to_string(),
            METHOD_SYNCING.to_string(),
            METHOD_LOGS.to_string(),
        ]
    }
}

/// Resolve the raw header JSON to source a head's `newHeads` header fields from.
///
/// Direct upstreams attach the header (or the full block) to the head, so this
/// is a cheap clone. A head sourced from a remote Dshackle's `ChainHead` carries
/// no header fields at all, so fetch the full block by hash — through the normal
/// call path, so it hits the cache — and use that. Returns `None` when nothing
/// is available; [`new_head_message`] then degrades to metadata only.
async fn head_raw_header(block: &BlockContainer, access: &dyn ChainAccess) -> Option<Arc<[u8]>> {
    if let Some(raw) = block.header_json.clone().or_else(|| block.json.clone()) {
        return Some(raw);
    }
    fetch_block_json(access, &block.hash).await
}

/// Fetch a full block by hash (`eth_getBlockByHash` with tx hashes only) as raw
/// JSON. Any failure — call error, JSON-RPC error, or an unknown block (`null`
/// result) — yields `None` rather than tearing down the subscription.
async fn fetch_block_json(access: &dyn ChainAccess, hash: &BlockId) -> Option<Arc<[u8]>> {
    let request = JsonRpcRequest::new(
        0,
        RpcMethod::from("eth_getBlockByHash"),
        json!([hash.to_hex_prefixed(), false]),
    );
    let response = access.call(&request).await.ok()?;
    if response.error.is_some() {
        return None;
    }
    let raw = response.result?;
    // `eth_getBlockByHash` answers `null` for an unknown block — not a header.
    if raw.get().trim() == "null" {
        return None;
    }
    Some(Arc::from(raw.get().as_bytes()))
}

/// Build a `newHeads` message from a head block and its resolved raw header.
///
/// The core fields come from the parsed block metadata; the remaining header
/// fields are passed through verbatim from `raw_header` (a `newHeads`
/// notification, the full block JSON, or a block fetched by hash — all carry the
/// same header fields, already encoded as canonical hex by the node). When
/// `raw_header` is `None` the message degrades to the metadata-only subset
/// rather than guessing.
fn new_head_message(block: &BlockContainer, raw_header: Option<&[u8]>) -> Vec<u8> {
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

    if let Some(raw) = raw_header
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
/// value each time the chain's status flips. Driven by the chain's
/// status-change signal (legacy `observeStatus`) rather than a poll, so a flip
/// and recovery in quick succession is never collapsed away.
fn syncing_stream(access: Arc<dyn ChainAccess>) -> SubscriptionStream {
    let changes = access.status_changes();
    let stream = futures::stream::unfold(
        (access, changes, Option::<bool>::None),
        |(access, mut changes, last)| async move {
            loop {
                let syncing = access.is_syncing();
                if last != Some(syncing) {
                    let msg = serde_json::to_vec(&serde_json::Value::Bool(syncing))
                        .expect("bool always serializes");
                    return Some((msg, (access, changes, Some(syncing))));
                }
                // No change yet — wait for the next status signal. `false` means
                // the chain is gone, so end the stream.
                if !changes.changed().await {
                    return None;
                }
            }
        },
    );
    Box::pin(stream)
}

/// `eth_subscribe("logs")` filter: the `address` / `topics` fields from the
/// subscription params, passed through to `eth_getLogs` verbatim so the node
/// applies the full standard topic semantics (positional AND, per-position OR,
/// `null` wildcards) rather than the legacy proxy's first-topic-only filter.
#[derive(Clone, Default)]
struct LogsFilter {
    address: Option<serde_json::Value>,
    topics: Option<serde_json::Value>,
}

impl LogsFilter {
    fn from_params(params: Option<serde_json::Value>) -> Self {
        let Some(obj) = params.as_ref().and_then(|v| v.as_object()) else {
            return Self::default();
        };
        Self {
            address: obj.get("address").cloned(),
            topics: obj.get("topics").cloned(),
        }
    }

    /// The `eth_getLogs` filter object scoped to a single block by hash.
    fn for_block(&self, block_hash: &BlockId) -> serde_json::Value {
        let mut filter = serde_json::Map::new();
        filter.insert("blockHash".into(), json!(block_hash.to_hex_prefixed()));
        if let Some(address) = &self.address {
            filter.insert("address".into(), address.clone());
        }
        if let Some(topics) = &self.topics {
            filter.insert("topics".into(), topics.clone());
        }
        serde_json::Value::Object(filter)
    }
}

/// Stream of `logs` messages: for each new head, fetch the block's matching
/// logs via `eth_getLogs` (scoped by `blockHash`) and emit each one.
///
/// Unlike the legacy proxy this delegates filtering to the node and reads whole
/// blocks at once instead of per-transaction receipts. Reorg `removed: true`
/// re-emission is not done yet — the merged head only advances forward and
/// reports no drops, so there is no signal to act on (tracked in the roadmap).
fn logs_stream(
    head: Arc<MergedHead>,
    access: Arc<dyn ChainAccess>,
    filter: LogsFilter,
) -> SubscriptionStream {
    struct State {
        rx: broadcast::Receiver<Arc<BlockContainer>>,
        access: Arc<dyn ChainAccess>,
        filter: LogsFilter,
        pending: VecDeque<Vec<u8>>,
    }

    let state = State {
        rx: head.subscribe(),
        access,
        filter,
        pending: VecDeque::new(),
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(msg) = state.pending.pop_front() {
                return Some((msg, state));
            }
            match state.rx.recv().await {
                Ok(block) => {
                    let logs =
                        fetch_block_logs(&*state.access, &block.hash, &state.filter).await;
                    state.pending.extend(logs);
                }
                // Skip the gap marker when a slow subscriber falls behind.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Box::pin(stream)
}

/// Fetch the logs of one block matching the filter and render each as a
/// subscription message. A failed call or non-array result yields no logs for
/// that block rather than tearing down the subscription.
async fn fetch_block_logs(
    access: &dyn ChainAccess,
    block_hash: &BlockId,
    filter: &LogsFilter,
) -> Vec<Vec<u8>> {
    let request = JsonRpcRequest::new(
        0,
        RpcMethod::from("eth_getLogs"),
        json!([filter.for_block(block_hash)]),
    );
    let response = match access.call(&request).await {
        Ok(response) => response,
        Err(_) => return Vec::new(),
    };
    if response.error.is_some() {
        return Vec::new();
    }
    let Some(result) = response.result else {
        return Vec::new();
    };
    let logs: Vec<serde_json::Value> = match serde_json::from_str(result.get()) {
        Ok(logs) => logs,
        Err(_) => return Vec::new(),
    };
    logs.into_iter().filter_map(log_message).collect()
}

/// Render one `eth_getLogs` result entry as a `logs` subscription message,
/// ensuring the `removed` flag is present (false for a canonical block).
fn log_message(mut log: serde_json::Value) -> Option<Vec<u8>> {
    if let serde_json::Value::Object(ref mut fields) = log {
        fields
            .entry("removed")
            .or_insert(serde_json::Value::Bool(false));
    }
    serde_json::to_vec(&log).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::head::CurrentHead;
    use crate::upstream::status_signal::StatusSignal;
    use std::sync::Mutex;
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

    /// Stub chain access: a settable syncing flag (changes wake the status
    /// signal, like a real upstream), a canned `eth_getLogs` result, and an
    /// optional `eth_getBlockByHash` result, capturing the params of the last
    /// call.
    struct StubAccess {
        syncing: AtomicBool,
        signal: StatusSignal,
        logs: Vec<serde_json::Value>,
        block_by_hash: Option<serde_json::Value>,
        last_params: Mutex<Option<serde_json::Value>>,
    }

    impl StubAccess {
        fn new(syncing: bool, logs: Vec<serde_json::Value>) -> Arc<Self> {
            Arc::new(Self {
                syncing: AtomicBool::new(syncing),
                signal: StatusSignal::new(),
                logs,
                block_by_hash: None,
                last_params: Mutex::new(None),
            })
        }
        fn with_block(syncing: bool, block_by_hash: serde_json::Value) -> Arc<Self> {
            Arc::new(Self {
                syncing: AtomicBool::new(syncing),
                signal: StatusSignal::new(),
                logs: vec![],
                block_by_hash: Some(block_by_hash),
                last_params: Mutex::new(None),
            })
        }
        fn set(&self, syncing: bool) {
            self.syncing.store(syncing, Ordering::Relaxed);
            self.signal.notify();
        }
        fn last_params(&self) -> Option<serde_json::Value> {
            self.last_params.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl ChainAccess for StubAccess {
        fn is_syncing(&self) -> bool {
            self.syncing.load(Ordering::Relaxed)
        }
        fn status_changes(&self) -> StatusChanges {
            self.signal.subscribe()
        }
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            *self.last_params.lock().unwrap() = Some(request.params.clone());
            let result = if request.method.as_ref() == "eth_getBlockByHash" {
                self.block_by_hash
                    .clone()
                    .unwrap_or(serde_json::Value::Null)
            } else {
                serde_json::Value::Array(self.logs.clone())
            };
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                serde_json::to_string(&result).unwrap()
            );
            Ok(serde_json::from_str(&body).unwrap())
        }
    }

    fn egress(head: Arc<MergedHead>, access: Arc<dyn ChainAccess>) -> EthereumEgress {
        EthereumEgress::new(head, access)
    }

    #[test]
    fn new_head_message_carries_full_header_fields() {
        let block = block_with_header(0x10, Some(HEADER_JSON));
        let msg = new_head_message(&block, block.header_json.as_deref());
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
        let msg = new_head_message(&block_with_header(0x10, None), None);
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
        let msg = new_head_message(&block, None);
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert!(value.as_object().unwrap().contains_key("parentHash"));
        assert!(value["parentHash"].is_null());
    }

    #[tokio::test]
    async fn new_heads_streams_head_messages() {
        let head = Arc::new(CurrentHead::new());
        let merged = MergedHead::new(vec![Arc::clone(&head)]);
        let egress = egress(merged, StubAccess::new(false, vec![]));

        let mut stream = egress.subscribe(METHOD_NEW_HEADS, None).unwrap();
        head.update_with_block(block_with_header(0x20, Some(HEADER_JSON)));

        let msg = stream.next().await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert_eq!(value["number"], "0x20");
        assert_eq!(value["gasLimit"], "0x1c9c380");
    }

    #[tokio::test]
    async fn new_heads_fetches_header_for_headerless_remote_dshackle_head() {
        // A head from a remote Dshackle's `ChainHead` carries no header; the
        // egress fetches the full block by hash to recover the header fields.
        let head = Arc::new(CurrentHead::new());
        let merged = MergedHead::new(vec![Arc::clone(&head)]);
        let block_by_hash: serde_json::Value = serde_json::from_str(HEADER_JSON).unwrap();
        let access = StubAccess::with_block(false, block_by_hash);
        let egress = egress(merged, Arc::clone(&access) as Arc<dyn ChainAccess>);

        let mut stream = egress.subscribe(METHOD_NEW_HEADS, None).unwrap();
        head.update_with_block(block_with_header(0x20, None));

        let msg = stream.next().await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert_eq!(value["number"], "0x20");
        // Header fields came from the fetched block, not the (headerless) head.
        assert_eq!(value["gasLimit"], "0x1c9c380");
        assert_eq!(value["miner"], "0xcafe");
        // The fetch was scoped to the head's block hash.
        let params = access.last_params().expect("eth_getBlockByHash was called");
        assert!(params[0].as_str().unwrap().starts_with("0x"));
        assert_eq!(params[1], false);
    }

    #[tokio::test]
    async fn syncing_emits_initial_then_on_change() {
        let access = StubAccess::new(false, vec![]);
        let merged = MergedHead::new(vec![]);
        let egress = egress(merged, Arc::clone(&access) as Arc<dyn ChainAccess>);

        let mut stream = egress.subscribe(METHOD_SYNCING, None).unwrap();

        // Initial value is emitted immediately.
        let first: serde_json::Value = serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
        assert_eq!(first, serde_json::json!(false));

        // `set` wakes the status signal; the stream re-checks and emits the new
        // value with no interval to wait on.
        access.set(true);
        let second: serde_json::Value =
            serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
        assert_eq!(second, serde_json::json!(true));
    }

    #[tokio::test]
    async fn syncing_wakes_a_parked_subscriber() {
        // A subscriber already awaiting (nothing has changed) is woken by the
        // status signal from another task — the event-driven path, not a poll.
        let access = StubAccess::new(false, vec![]);
        let merged = MergedHead::new(vec![]);
        let egress = egress(merged, Arc::clone(&access) as Arc<dyn ChainAccess>);
        let mut stream = egress.subscribe(METHOD_SYNCING, None).unwrap();

        // Drain the initial `false`, leaving the stream parked on the signal.
        let first: serde_json::Value =
            serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
        assert_eq!(first, serde_json::json!(false));

        // Park the next read, then flip the status from another task.
        let next = tokio::spawn(async move { stream.next().await });
        let flipper = tokio::spawn(async move {
            // Yield so the reader reaches its await before we signal.
            tokio::task::yield_now().await;
            access.set(true);
        });
        flipper.await.unwrap();

        let msg = next.await.unwrap().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert_eq!(value, serde_json::json!(true));
    }

    #[tokio::test]
    async fn logs_stream_emits_block_logs_and_passes_filter() {
        let head = Arc::new(CurrentHead::new());
        let merged = MergedHead::new(vec![Arc::clone(&head)]);
        let canned = vec![json!({
            "address": "0xabc",
            "topics": ["0x1"],
            "data": "0x",
            "logIndex": "0x0"
        })];
        let access = StubAccess::new(false, canned);
        let egress = egress(merged, Arc::clone(&access) as Arc<dyn ChainAccess>);

        let mut stream = egress
            .subscribe(
                METHOD_LOGS,
                Some(json!({"address": "0xabc", "topics": ["0x1"]})),
            )
            .unwrap();
        head.update_with_block(block_with_header(0x10, None));

        let msg = stream.next().await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert_eq!(value["address"], "0xabc");
        // `removed` is added when the node's result omits it.
        assert_eq!(value["removed"], false);

        // The filter reached eth_getLogs, scoped to the block by hash.
        let params = access.last_params().expect("eth_getLogs was called");
        let filter = &params[0];
        assert!(filter["blockHash"].as_str().unwrap().starts_with("0x"));
        assert_eq!(filter["address"], "0xabc");
        assert_eq!(filter["topics"], json!(["0x1"]));
    }

    #[tokio::test]
    async fn logs_without_filter_query_only_scopes_by_block() {
        let head = Arc::new(CurrentHead::new());
        let merged = MergedHead::new(vec![Arc::clone(&head)]);
        let access = StubAccess::new(false, vec![json!({"address": "0x1"})]);
        let egress = egress(merged, Arc::clone(&access) as Arc<dyn ChainAccess>);

        let mut stream = egress.subscribe(METHOD_LOGS, None).unwrap();
        head.update_with_block(block_with_header(0x10, None));
        let _ = stream.next().await.unwrap();

        let filter = access.last_params().unwrap()[0].clone();
        assert!(filter.get("blockHash").is_some());
        assert!(filter.get("address").is_none());
        assert!(filter.get("topics").is_none());
    }

    #[tokio::test]
    async fn unsupported_method_errors() {
        let merged = MergedHead::new(vec![]);
        let egress = egress(merged, StubAccess::new(false, vec![]));
        let err = egress
            .subscribe("newPendingTransactions", None)
            .err()
            .expect("newPendingTransactions is not supported yet");
        assert!(matches!(err, EgressError::UnsupportedMethod(m) if m == "newPendingTransactions"));
    }
}
