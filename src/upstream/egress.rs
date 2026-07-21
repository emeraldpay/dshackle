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
use crate::upstream::block_updates::{BlockUpdate, WINDOW_LIMIT};
#[cfg(test)]
use crate::upstream::merged_head::MergeOrder;
use crate::upstream::merged_head::MergedHead;
use crate::upstream::status_signal::StatusChanges;
use crate::upstream::traits::UpstreamError;
use itertools::Itertools;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
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

/// The egress sources of one chain folded into a single egress — the local
/// egress plus any remote Dshackle relays. A topic carried by several sources
/// merges their streams into one, as the legacy chain egress merged every
/// upstream's stream of a topic.
pub struct MergedEgress {
    sources: Vec<Arc<dyn EgressSubscription>>,
}

impl MergedEgress {
    /// Collapse a chain's egress sources into one: `None` when there are
    /// none, the sole source unchanged, or a merging wrapper.
    pub fn combined(
        sources: Vec<Arc<dyn EgressSubscription>>,
    ) -> Option<Arc<dyn EgressSubscription>> {
        match sources.len() {
            0 => None,
            1 => sources.into_iter().next(),
            _ => Some(Arc::new(MergedEgress { sources })),
        }
    }
}

impl EgressSubscription for MergedEgress {
    fn subscribe(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<SubscriptionStream, EgressError> {
        let mut streams = Vec::new();
        for source in &self.sources {
            if let Ok(stream) = source.subscribe(method, params.clone()) {
                streams.push(stream);
            }
        }
        match streams.len() {
            0 => Err(EgressError::UnsupportedMethod(method.to_string())),
            1 => Ok(streams.remove(0)),
            _ => Ok(Box::pin(futures::stream::select_all(streams))),
        }
    }

    fn available_topics(&self) -> Vec<String> {
        self.sources
            .iter()
            .flat_map(|source| source.available_topics())
            .unique()
            .collect()
    }
}

/// A view of an egress restricted to a subset of its topics.
///
/// Cuts from a remote Dshackle relay the Ethereum topics the local egress
/// already serves: those are derived from the chain head the remote itself
/// feeds (over `SubscribeHead`), so relaying them as well would deliver every
/// message twice.
pub struct TopicSubset {
    inner: Arc<dyn EgressSubscription>,
    topics: Vec<String>,
}

impl TopicSubset {
    /// `topics` is the visible subset: a topic outside it is rejected even
    /// when `inner` could serve it.
    pub fn new(inner: Arc<dyn EgressSubscription>, topics: Vec<String>) -> Self {
        Self { inner, topics }
    }
}

impl EgressSubscription for TopicSubset {
    fn subscribe(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<SubscriptionStream, EgressError> {
        if !self.topics.iter().any(|t| t == method) {
            return Err(EgressError::UnsupportedMethod(method.to_string()));
        }
        self.inner.subscribe(method, params)
    }

    fn available_topics(&self) -> Vec<String> {
        self.topics.clone()
    }
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

    /// Whether any upstream of the chain can serve `method` at all. The
    /// `logs` topic depends on `eth_getLogs` being callable: advertising it
    /// on a chain that structurally rejects the method would accept
    /// subscriptions that can only starve ([`logs_stream`] absorbs the
    /// per-block failures by design). Defaults to `true` for accessors
    /// without method routing.
    fn serves_method(&self, _method: &RpcMethod) -> bool {
        true
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

    /// `newHeads` and `syncing` need only the head and status this egress
    /// already holds, but `logs` is a promise to call `eth_getLogs` per
    /// block — kept out of the advertised topics when the chain can't make
    /// that call, so a subscriber gets a clean rejection (and a relayed
    /// `logs` isn't shadowed) instead of an eternally silent stream.
    fn serves_logs(&self) -> bool {
        self.access.serves_method(&RpcMethod::from("eth_getLogs"))
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
            METHOD_LOGS if self.serves_logs() => Ok(logs_stream(
                Arc::clone(&self.head),
                Arc::clone(&self.access),
                LogsFilter::from_params(params),
            )),
            other => Err(EgressError::UnsupportedMethod(other.to_string())),
        }
    }

    fn available_topics(&self) -> Vec<String> {
        let mut topics = vec![METHOD_NEW_HEADS.to_string(), METHOD_SYNCING.to_string()];
        if self.serves_logs() {
            topics.push(METHOD_LOGS.to_string());
        }
        topics
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

/// Per-subscription cache of the logs emitted for recent blocks, kept so a
/// reorg can replay them flagged `removed` — an orphaned block's receipts are
/// no longer served by the node, so refetching is not an option (same reason
/// the legacy `ProduceLogs` kept its cache). Sized to the reorg-detection
/// window: a drop can only ever arrive for a block still inside it.
struct RecentBlockLogs {
    by_block: HashMap<BlockId, Vec<serde_json::Value>>,
    order: VecDeque<BlockId>,
}

impl RecentBlockLogs {
    fn new() -> Self {
        Self {
            by_block: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn remember(&mut self, block: BlockId, logs: Vec<serde_json::Value>) {
        if logs.is_empty() {
            return;
        }
        if self.by_block.insert(block, logs).is_none() {
            self.order.push_back(block);
        }
        if self.order.len() > WINDOW_LIMIT
            && let Some(evicted) = self.order.pop_front()
        {
            self.by_block.remove(&evicted);
        }
    }

    /// The cached logs of a dropped block. Empty when the block had no
    /// matching logs or the subscription started after the block was announced
    /// — nothing was emitted, so there is nothing to retract.
    fn take(&mut self, block: &BlockId) -> Vec<serde_json::Value> {
        match self.by_block.remove(block) {
            Some(logs) => {
                self.order.retain(|b| b != block);
                logs
            }
            None => Vec::new(),
        }
    }
}

/// Stream of `logs` messages: for each new canonical block, fetch the block's
/// matching logs via `eth_getLogs` (scoped by `blockHash`) and emit each one;
/// when a reorg drops a block, replay its cached logs with `removed: true`
/// before the replacement block's logs (drops arrive first on the updates
/// channel). Ports the legacy `ConnectLogs`/`ProduceLogs` behavior.
///
/// Unlike the legacy proxy this delegates filtering to the node and reads whole
/// blocks at once instead of per-transaction receipts, so the replay cache
/// holds the post-filter logs of this subscription.
fn logs_stream(
    head: Arc<MergedHead>,
    access: Arc<dyn ChainAccess>,
    filter: LogsFilter,
) -> SubscriptionStream {
    struct State {
        rx: broadcast::Receiver<BlockUpdate>,
        access: Arc<dyn ChainAccess>,
        filter: LogsFilter,
        recent: RecentBlockLogs,
        pending: VecDeque<Vec<u8>>,
    }

    let state = State {
        rx: head.subscribe_updates(),
        access,
        filter,
        recent: RecentBlockLogs::new(),
        pending: VecDeque::new(),
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(msg) = state.pending.pop_front() {
                return Some((msg, state));
            }
            match state.rx.recv().await {
                Ok(BlockUpdate::New(block)) => {
                    let logs = fetch_block_logs(&*state.access, &block.hash, &state.filter).await;
                    state.pending.extend(
                        logs.iter()
                            .filter_map(|log| log_message(log.clone(), false)),
                    );
                    state.recent.remember(block.hash, logs);
                }
                Ok(BlockUpdate::Drop(block)) => {
                    state.pending.extend(
                        state
                            .recent
                            .take(&block.hash)
                            .into_iter()
                            .filter_map(|log| log_message(log, true)),
                    );
                }
                // Skip the gap marker when a slow subscriber falls behind.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Box::pin(stream)
}

/// Fetch the logs of one block matching the filter, as raw log objects. A
/// failed call or non-array result yields no logs for that block rather than
/// tearing down the subscription.
async fn fetch_block_logs(
    access: &dyn ChainAccess,
    block_hash: &BlockId,
    filter: &LogsFilter,
) -> Vec<serde_json::Value> {
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
    serde_json::from_str(result.get()).unwrap_or_default()
}

/// Render one log as a `logs` subscription message. For a canonical block the
/// `removed` flag is only filled in when the node omitted it; for a dropped
/// block it is forced to `true` no matter what was originally announced.
fn log_message(mut log: serde_json::Value, removed: bool) -> Option<Vec<u8>> {
    if let serde_json::Value::Object(ref mut fields) = log {
        if removed {
            fields.insert("removed".into(), serde_json::Value::Bool(true));
        } else {
            fields
                .entry("removed")
                .or_insert(serde_json::Value::Bool(false));
        }
    }
    serde_json::to_vec(&log).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
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
        block_at(height, height as u8, header)
    }

    fn block_at(height: u64, hash_byte: u8, header: Option<&str>) -> BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
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
    /// signal, like a real upstream), a canned `eth_getLogs` result (global or
    /// per block hash), and an optional `eth_getBlockByHash` result, capturing
    /// the params of the last call.
    struct StubAccess {
        syncing: AtomicBool,
        signal: StatusSignal,
        logs: Vec<serde_json::Value>,
        logs_by_block: Mutex<HashMap<String, Vec<serde_json::Value>>>,
        block_by_hash: Option<serde_json::Value>,
        last_params: Mutex<Option<serde_json::Value>>,
        /// Whether the chain can route `eth_getLogs` (the `serves_method`
        /// answer for it).
        logs_callable: bool,
    }

    impl StubAccess {
        fn new(syncing: bool, logs: Vec<serde_json::Value>) -> Arc<Self> {
            Arc::new(Self {
                syncing: AtomicBool::new(syncing),
                signal: StatusSignal::new(),
                logs,
                logs_by_block: Mutex::new(HashMap::new()),
                block_by_hash: None,
                last_params: Mutex::new(None),
                logs_callable: true,
            })
        }
        fn without_get_logs() -> Arc<Self> {
            Arc::new(Self {
                syncing: AtomicBool::new(false),
                signal: StatusSignal::new(),
                logs: vec![],
                logs_by_block: Mutex::new(HashMap::new()),
                block_by_hash: None,
                last_params: Mutex::new(None),
                logs_callable: false,
            })
        }
        fn with_block(syncing: bool, block_by_hash: serde_json::Value) -> Arc<Self> {
            Arc::new(Self {
                syncing: AtomicBool::new(syncing),
                signal: StatusSignal::new(),
                logs: vec![],
                logs_by_block: Mutex::new(HashMap::new()),
                block_by_hash: Some(block_by_hash),
                last_params: Mutex::new(None),
                logs_callable: true,
            })
        }
        fn set(&self, syncing: bool) {
            self.syncing.store(syncing, Ordering::Relaxed);
            self.signal.notify();
        }
        fn set_logs_for(&self, block_hash: String, logs: Vec<serde_json::Value>) {
            self.logs_by_block.lock().unwrap().insert(block_hash, logs);
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
        fn serves_method(&self, method: &RpcMethod) -> bool {
            self.logs_callable || method.as_ref() != "eth_getLogs"
        }
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            *self.last_params.lock().unwrap() = Some(request.params.clone());
            let result = if request.method.as_ref() == "eth_getBlockByHash" {
                self.block_by_hash
                    .clone()
                    .unwrap_or(serde_json::Value::Null)
            } else {
                let by_block = self.logs_by_block.lock().unwrap();
                let per_block = request.params[0]["blockHash"]
                    .as_str()
                    .and_then(|hash| by_block.get(hash).cloned());
                serde_json::Value::Array(per_block.unwrap_or_else(|| self.logs.clone()))
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
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let egress = egress(Arc::clone(&merged), StubAccess::new(false, vec![]));

        let mut stream = egress.subscribe(METHOD_NEW_HEADS, None).unwrap();
        merged.feed(0, Arc::new(block_with_header(0x20, Some(HEADER_JSON))));

        let msg = stream.next().await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert_eq!(value["number"], "0x20");
        assert_eq!(value["gasLimit"], "0x1c9c380");
    }

    #[tokio::test]
    async fn new_heads_fetches_header_for_headerless_remote_dshackle_head() {
        // A head from a remote Dshackle's `ChainHead` carries no header; the
        // egress fetches the full block by hash to recover the header fields.
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let block_by_hash: serde_json::Value = serde_json::from_str(HEADER_JSON).unwrap();
        let access = StubAccess::with_block(false, block_by_hash);
        let egress = egress(
            Arc::clone(&merged),
            Arc::clone(&access) as Arc<dyn ChainAccess>,
        );

        let mut stream = egress.subscribe(METHOD_NEW_HEADS, None).unwrap();
        merged.feed(0, Arc::new(block_with_header(0x20, None)));

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
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let egress = egress(
            Arc::clone(&merged),
            Arc::clone(&access) as Arc<dyn ChainAccess>,
        );

        let mut stream = egress.subscribe(METHOD_SYNCING, None).unwrap();

        // Initial value is emitted immediately.
        let first: serde_json::Value =
            serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
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
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let egress = egress(
            Arc::clone(&merged),
            Arc::clone(&access) as Arc<dyn ChainAccess>,
        );
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
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let canned = vec![json!({
            "address": "0xabc",
            "topics": ["0x1"],
            "data": "0x",
            "logIndex": "0x0"
        })];
        let access = StubAccess::new(false, canned);
        let egress = egress(
            Arc::clone(&merged),
            Arc::clone(&access) as Arc<dyn ChainAccess>,
        );

        let mut stream = egress
            .subscribe(
                METHOD_LOGS,
                Some(json!({"address": "0xabc", "topics": ["0x1"]})),
            )
            .unwrap();
        merged.feed(0, Arc::new(block_with_header(0x10, None)));

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
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let access = StubAccess::new(false, vec![json!({"address": "0x1"})]);
        let egress = egress(
            Arc::clone(&merged),
            Arc::clone(&access) as Arc<dyn ChainAccess>,
        );

        let mut stream = egress.subscribe(METHOD_LOGS, None).unwrap();
        merged.feed(0, Arc::new(block_with_header(0x10, None)));
        let _ = stream.next().await.unwrap();

        let filter = access.last_params().unwrap()[0].clone();
        assert!(filter.get("blockHash").is_some());
        assert!(filter.get("address").is_none());
        assert!(filter.get("topics").is_none());
    }

    #[tokio::test]
    async fn logs_reorg_replays_dropped_logs_as_removed() {
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let access = StubAccess::new(false, vec![]);

        let original = block_at(0x10, 1, None);
        let replacement = block_at(0x10, 2, None);
        access.set_logs_for(
            original.hash.to_hex_prefixed(),
            vec![json!({"address": "0xaaa", "logIndex": "0x0"})],
        );
        access.set_logs_for(
            replacement.hash.to_hex_prefixed(),
            vec![json!({"address": "0xbbb", "logIndex": "0x0"})],
        );

        let egress = egress(
            Arc::clone(&merged),
            Arc::clone(&access) as Arc<dyn ChainAccess>,
        );
        let mut stream = egress.subscribe(METHOD_LOGS, None).unwrap();

        merged.feed(0, Arc::new(original));
        let first: serde_json::Value =
            serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
        assert_eq!(first["address"], "0xaaa");
        assert_eq!(first["removed"], false);

        // The reorg retracts the original block's logs before announcing the
        // replacement's.
        merged.feed(0, Arc::new(replacement));
        let dropped: serde_json::Value =
            serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
        assert_eq!(dropped["address"], "0xaaa");
        assert_eq!(dropped["removed"], true);

        let added: serde_json::Value =
            serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
        assert_eq!(added["address"], "0xbbb");
        assert_eq!(added["removed"], false);
    }

    #[tokio::test]
    async fn logs_reorg_without_cached_logs_emits_only_replacement() {
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let access = StubAccess::new(false, vec![]);

        // The original block has no matching logs, so its drop has nothing to
        // retract and the stream goes straight to the replacement's logs.
        let original = block_at(0x10, 1, None);
        let replacement = block_at(0x10, 2, None);
        access.set_logs_for(
            replacement.hash.to_hex_prefixed(),
            vec![json!({"address": "0xbbb", "logIndex": "0x0"})],
        );

        let egress = egress(
            Arc::clone(&merged),
            Arc::clone(&access) as Arc<dyn ChainAccess>,
        );
        let mut stream = egress.subscribe(METHOD_LOGS, None).unwrap();

        merged.feed(0, Arc::new(original));
        merged.feed(0, Arc::new(replacement));

        let msg: serde_json::Value = serde_json::from_slice(&stream.next().await.unwrap()).unwrap();
        assert_eq!(msg["address"], "0xbbb");
        assert_eq!(msg["removed"], false);
    }

    /// An egress serving one topic with one fixed message, for combinator
    /// tests.
    struct FixedEgress {
        topic: &'static str,
        message: &'static str,
    }

    impl EgressSubscription for FixedEgress {
        fn subscribe(
            &self,
            method: &str,
            _params: Option<serde_json::Value>,
        ) -> Result<SubscriptionStream, EgressError> {
            if method != self.topic {
                return Err(EgressError::UnsupportedMethod(method.to_string()));
            }
            let message = self.message.as_bytes().to_vec();
            Ok(Box::pin(futures::stream::once(async move { message })))
        }

        fn available_topics(&self) -> Vec<String> {
            vec![self.topic.to_string()]
        }
    }

    #[test]
    fn merged_egress_collapses_none_and_single() {
        assert!(MergedEgress::combined(vec![]).is_none());

        let single = MergedEgress::combined(vec![Arc::new(FixedEgress {
            topic: "hashtx",
            message: "a",
        })])
        .unwrap();
        assert_eq!(single.available_topics(), vec!["hashtx".to_string()]);
    }

    #[test]
    fn merged_egress_unions_topics_without_duplicates() {
        let merged = MergedEgress::combined(vec![
            Arc::new(FixedEgress {
                topic: "hashtx",
                message: "a",
            }),
            Arc::new(FixedEgress {
                topic: "hashtx",
                message: "b",
            }),
            Arc::new(FixedEgress {
                topic: "rawtx",
                message: "c",
            }),
        ])
        .unwrap();
        assert_eq!(
            merged.available_topics(),
            vec!["hashtx".to_string(), "rawtx".to_string()]
        );
    }

    #[tokio::test]
    async fn merged_egress_merges_streams_of_a_shared_topic() {
        let merged = MergedEgress::combined(vec![
            Arc::new(FixedEgress {
                topic: "hashtx",
                message: "a",
            }),
            Arc::new(FixedEgress {
                topic: "hashtx",
                message: "b",
            }),
        ])
        .unwrap();

        let mut stream = merged.subscribe("hashtx", None).unwrap();
        let mut received = Vec::new();
        while let Some(msg) = stream.next().await {
            received.push(String::from_utf8(msg).unwrap());
        }
        received.sort();
        assert_eq!(received, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn merged_egress_serves_a_topic_of_one_source() {
        let merged = MergedEgress::combined(vec![
            Arc::new(FixedEgress {
                topic: "hashtx",
                message: "a",
            }),
            Arc::new(FixedEgress {
                topic: "rawtx",
                message: "c",
            }),
        ])
        .unwrap();

        let mut stream = merged.subscribe("rawtx", None).unwrap();
        assert_eq!(stream.next().await.unwrap(), b"c".to_vec());

        assert!(matches!(
            merged.subscribe("nonsense", None),
            Err(EgressError::UnsupportedMethod(m)) if m == "nonsense"
        ));
    }

    #[tokio::test]
    async fn topic_subset_masks_the_hidden_topics() {
        let subset = TopicSubset::new(
            Arc::new(FixedEgress {
                topic: "hashtx",
                message: "a",
            }),
            vec![],
        );
        assert!(subset.available_topics().is_empty());
        assert!(matches!(
            subset.subscribe("hashtx", None),
            Err(EgressError::UnsupportedMethod(m)) if m == "hashtx"
        ));

        let subset = TopicSubset::new(
            Arc::new(FixedEgress {
                topic: "hashtx",
                message: "a",
            }),
            vec!["hashtx".to_string()],
        );
        let mut stream = subset.subscribe("hashtx", None).unwrap();
        assert_eq!(stream.next().await.unwrap(), b"a".to_vec());
    }

    #[tokio::test]
    async fn logs_hidden_when_get_logs_is_not_callable() {
        // A chain that structurally rejects `eth_getLogs` must not promise
        // the `logs` topic: better a clean rejection (and an unshadowed
        // relay) than a subscription that silently never emits.
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let egress = egress(
            Arc::clone(&merged),
            StubAccess::without_get_logs() as Arc<dyn ChainAccess>,
        );

        assert_eq!(
            egress.available_topics(),
            vec![METHOD_NEW_HEADS.to_string(), METHOD_SYNCING.to_string()]
        );
        assert!(matches!(
            egress.subscribe(METHOD_LOGS, None),
            Err(EgressError::UnsupportedMethod(m)) if m == METHOD_LOGS
        ));
        // The head-derived topics are unaffected.
        assert!(egress.subscribe(METHOD_NEW_HEADS, None).is_ok());
    }

    #[tokio::test]
    async fn unsupported_method_errors() {
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let egress = egress(Arc::clone(&merged), StubAccess::new(false, vec![]));
        let err = egress
            .subscribe("newPendingTransactions", None)
            .err()
            .expect("newPendingTransactions is not supported yet");
        assert!(matches!(err, EgressError::UnsupportedMethod(m) if m == "newPendingTransactions"));
    }
}
