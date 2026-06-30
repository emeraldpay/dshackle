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

//! Implementation of the `Blockchain` gRPC service from the emerald-api.
//!
//! Implemented so far: `native_call`, `subscribe_head`, and `native_subscribe`
//! (newHeads). The remaining methods return `UNIMPLEMENTED`.

use crate::blockchain::TargetBlockchain;
use crate::data::BlockContainer;
use crate::rpc::native_call;
use crate::upstream::traits::Capability;
use crate::upstream::{Multistream, UpstreamManager};
use crate::upstream::egress::EgressError;
use crate::upstream::fees::{FeeError, FeeMode};
use emerald_api::proto::blockchain::blockchain_server::Blockchain;
use emerald_api::proto::blockchain::*;
use emerald_api::proto::common;
use futures::stream::{self, FuturesUnordered, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::Stream;
use tokio_stream::wrappers::BroadcastStream;

/// How often [`subscribe_status`] re-checks a configured chain's aggregate
/// status while it is unchanged. The legacy `SubscribeStatus` is event-driven
/// off `Multistream.observeStatus`; until an event-driven status signal exists
/// (roadmap 4.1) we poll, the same approach as the `syncing` egress.
const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The Dshackle implementation of the `Blockchain` gRPC service.
pub struct BlockchainRpcService {
    upstreams: Arc<UpstreamManager>,
}

impl BlockchainRpcService {
    pub fn new(upstreams: Arc<UpstreamManager>) -> Self {
        Self { upstreams }
    }
}

// A boxed stream type used for all streaming responses.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl Blockchain for BlockchainRpcService {
    type SubscribeHeadStream = BoxStream<ChainHead>;
    type SubscribeBalanceStream = BoxStream<AddressBalance>;
    type SubscribeTxStatusStream = BoxStream<TxStatus>;
    type GetBalanceStream = BoxStream<AddressBalance>;
    type GetAddressAllowanceStream = BoxStream<AddressAllowance>;
    type SubscribeAddressAllowanceStream = BoxStream<AddressAllowance>;
    type NativeCallStream = BoxStream<NativeCallReplyItem>;
    type NativeSubscribeStream = BoxStream<NativeSubscribeReplyItem>;
    type SubscribeStatusStream = BoxStream<ChainStatus>;

    async fn native_call(
        &self,
        request: tonic::Request<NativeCallRequest>,
    ) -> Result<tonic::Response<Self::NativeCallStream>, tonic::Status> {
        let req = request.into_inner();
        tracing::trace!(
            chain = req.chain,
            items = req.items.len(),
            "native_call request"
        );

        let chain = TargetBlockchain::try_from(req.chain).map_err(|id| {
            tracing::trace!(chain = id, "unknown chain id");
            tonic::Status::invalid_argument(format!("unknown chain id {id}"))
        })?;

        let multistream = self
            .upstreams
            .get(&chain)
            .ok_or_else(|| {
                tracing::trace!(%chain, "no upstream for chain");
                tonic::Status::unavailable(format!("no upstream available for chain {chain}"))
            })?
            .clone();

        // Each item is dispatched independently and in parallel.
        // Replies stream out in completion order; the client correlates them via `NativeCallReplyItem.id`.
        let tasks: FuturesUnordered<_> = req
            .items
            .into_iter()
            .map(|item| {
                let multistream = multistream.clone();
                tokio::spawn(async move {
                    native_call::execute_native_call(multistream.as_ref(), &item).await
                })
            })
            .collect();

        let stream = tasks.map(|joined| match joined {
            Ok(reply) => Ok(reply),
            Err(e) => Err(tonic::Status::internal(format!(
                "native_call task failed: {e}"
            ))),
        });

        Ok(tonic::Response::new(Box::pin(stream)))
    }

    // ── All other methods are not yet implemented ────────────────────────

    async fn subscribe_head(
        &self,
        request: tonic::Request<common::Chain>,
    ) -> Result<tonic::Response<Self::SubscribeHeadStream>, tonic::Status> {
        let chain = TargetBlockchain::try_from(request.into_inner().r#type).map_err(|id| {
            tracing::trace!(chain = id, "unknown chain id");
            tonic::Status::invalid_argument(format!("unknown chain id {id}"))
        })?;

        let head = self.upstreams.head(&chain).ok_or_else(|| {
            tracing::trace!(%chain, "no head stream for chain");
            tonic::Status::unavailable(format!("no upstream available for chain {chain}"))
        })?;

        let chain_id = chain.id();
        // Map each merged-head block to a `ChainHead`, skipping the gaps the
        // broadcast channel reports when a subscriber lags behind.
        let stream = BroadcastStream::new(head.subscribe()).filter_map(move |item| async move {
            item.ok().map(|block| Ok(chain_head(chain_id, &block)))
        });

        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn subscribe_balance(
        &self,
        _request: tonic::Request<BalanceRequest>,
    ) -> Result<tonic::Response<Self::SubscribeBalanceStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "subscribe_balance not yet implemented",
        ))
    }

    async fn subscribe_tx_status(
        &self,
        _request: tonic::Request<TxStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTxStatusStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "subscribe_tx_status not yet implemented",
        ))
    }

    async fn get_balance(
        &self,
        _request: tonic::Request<BalanceRequest>,
    ) -> Result<tonic::Response<Self::GetBalanceStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "get_balance not yet implemented",
        ))
    }

    async fn get_address_allowance(
        &self,
        _request: tonic::Request<AddressAllowanceRequest>,
    ) -> Result<tonic::Response<Self::GetAddressAllowanceStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "get_address_allowance not yet implemented",
        ))
    }

    async fn subscribe_address_allowance(
        &self,
        _request: tonic::Request<AddressAllowanceRequest>,
    ) -> Result<tonic::Response<Self::SubscribeAddressAllowanceStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "subscribe_address_allowance not yet implemented",
        ))
    }

    async fn estimate_fee(
        &self,
        request: tonic::Request<EstimateFeeRequest>,
    ) -> Result<tonic::Response<EstimateFeeResponse>, tonic::Status> {
        let req = request.into_inner();

        // An unconfigured (or non-fee-supporting, e.g. Bitcoin) chain is
        // `UNAVAILABLE`; the fee estimator is `None` in both cases. Carry the
        // raw requested chain value in the message, matching the legacy text.
        let fees = TargetBlockchain::try_from(req.chain)
            .ok()
            .and_then(|chain| self.upstreams.fees(&chain))
            .ok_or_else(|| {
                tonic::Status::unavailable(format!("BLOCKCHAIN UNAVAILABLE: {}", req.chain))
            })?;

        // Reject an unusable mode up front (legacy `ChainFees.extractMode`).
        let mode = FeeMode::from_proto(req.mode).ok_or_else(|| {
            tonic::Status::unavailable(format!("UNSUPPORTED MODE: {}", req.mode))
        })?;

        match fees.estimate(mode, req.blocks).await {
            Ok(response) => Ok(tonic::Response::new(response)),
            // No head yet or too little data to sample — the chain can't answer
            // right now (legacy returns an empty/!ready result here).
            Err(FeeError::NotReady) => {
                Err(tonic::Status::unavailable("upstream is not ready"))
            }
            Err(FeeError::NoData) => Err(tonic::Status::unavailable(
                "not enough data to estimate fee",
            )),
        }
    }

    async fn native_subscribe(
        &self,
        request: tonic::Request<NativeSubscribeRequest>,
    ) -> Result<tonic::Response<Self::NativeSubscribeStream>, tonic::Status> {
        let req = request.into_inner();

        let chain = TargetBlockchain::try_from(req.chain)
            .map_err(|id| tonic::Status::unavailable(format!("BLOCKCHAIN UNAVAILABLE: {id}")))?;
        let params = parse_subscribe_params(&req.payload)?;

        // No egress means the chain can't serve subscriptions: it tracks no
        // head yet, or it isn't an Ethereum-family chain (Bitcoin egress isn't
        // ported).
        let egress = self.upstreams.egress(&chain).ok_or_else(|| {
            tonic::Status::unavailable(format!("BLOCKCHAIN UNAVAILABLE: {chain}"))
        })?;

        match egress.subscribe(&req.method, params) {
            Ok(stream) => {
                let mapped = stream.map(|payload| Ok(NativeSubscribeReplyItem { payload }));
                Ok(tonic::Response::new(Box::pin(mapped)))
            }
            Err(EgressError::UnsupportedMethod(method)) => Err(tonic::Status::unimplemented(
                format!("Method {method} is not supported"),
            )),
        }
    }

    async fn describe(
        &self,
        _request: tonic::Request<DescribeRequest>,
    ) -> Result<tonic::Response<DescribeResponse>, tonic::Status> {
        // Discovery: advertise every configured chain with its status, the
        // methods/subscriptions it can serve, and its nodes. Mirrors the legacy
        // `Describe`. A Dshackle-behind-Dshackle upstream relies on `chain` and
        // `supported_methods` here, so those two must always be populated.
        let mut chains = Vec::new();
        for chain in self.upstreams.chains() {
            let Some(multistream) = self.upstreams.get(&chain) else {
                continue;
            };

            // One node per upstream with quorum 1 (legacy `getQuorumByLabel`),
            // carrying that upstream's configured labels.
            let nodes = multistream
                .upstreams()
                .iter()
                .map(|up| NodeDetails {
                    quorum: 1,
                    labels: up
                        .labels()
                        .iter()
                        .map(|(name, value)| Label {
                            name: name.clone(),
                            value: value.clone(),
                        })
                        .collect(),
                })
                .collect();

            let supported_subscriptions = self
                .upstreams
                .egress(&chain)
                .map(|e| e.available_topics())
                .unwrap_or_default();

            // Union of every upstream's capabilities (legacy `Multistream`
            // folds them the same way).
            let mut capabilities: Vec<i32> = Vec::new();
            for up in multistream.upstreams() {
                for cap in up.capabilities() {
                    let proto = capability_to_proto(cap);
                    if !capabilities.contains(&proto) {
                        capabilities.push(proto);
                    }
                }
            }

            chains.push(DescribeChain {
                chain: chain.id(),
                status: Some(chain_status(&chain, multistream)),
                nodes,
                supported_methods: multistream.supported_methods(),
                excluded_methods: Vec::new(),
                capabilities,
                supported_subscriptions,
            });
        }

        Ok(tonic::Response::new(DescribeResponse { chains }))
    }

    async fn subscribe_status(
        &self,
        request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeStatusStream>, tonic::Status> {
        let req = request.into_inner();

        // One sub-stream per requested chain, merged into a single response
        // stream (legacy `Flux.merge`). A configured chain streams its status
        // on every change; an unconfigured or unknown chain emits a single
        // `UNAVAILABLE` and completes (legacy `chainUnavailable`).
        let streams: Vec<Self::SubscribeStatusStream> = req
            .chains
            .into_iter()
            .map(|chain_id| match TargetBlockchain::try_from(chain_id) {
                Ok(chain) => match self.upstreams.get(&chain) {
                    Some(multistream) => chain_status_stream(chain, multistream.clone()),
                    None => once_status(chain_unavailable(chain.id())),
                },
                // An id outside the known `ChainRef` set still gets a definite
                // answer, carrying the requested id verbatim.
                Err(id) => once_status(chain_unavailable(id)),
            })
            .collect();

        Ok(tonic::Response::new(Box::pin(stream::select_all(streams))))
    }
}

/// A single-item status stream that emits `status` once and completes. Used for
/// chains with no configured upstreams.
fn once_status(status: ChainStatus) -> BoxStream<ChainStatus> {
    Box::pin(stream::once(std::future::ready(Ok(status))))
}

/// `ChainStatus` for a chain that has no upstreams: `UNAVAILABLE`, quorum 0
/// (legacy `SubscribeStatus.chainUnavailable`).
fn chain_unavailable(chain_id: i32) -> ChainStatus {
    use crate::upstream::availability::UpstreamAvailability;
    ChainStatus {
        chain: chain_id,
        availability: UpstreamAvailability::Unavailable as i32,
        quorum: 0,
    }
}

/// Stream a chain's [`ChainStatus`]: the current value immediately, then a new
/// value each time its aggregate availability or in-quorum upstream count
/// changes. Mirrors the legacy `SubscribeStatus` subscribing to
/// `Multistream.observeStatus`; polls on [`STATUS_POLL_INTERVAL`] while
/// unchanged (see the note there).
fn chain_status_stream(
    chain: TargetBlockchain,
    multistream: Arc<Multistream>,
) -> BoxStream<ChainStatus> {
    // `ChainStatus` is `Copy + Eq`, so we keep the last emitted value and only
    // yield on a real change — `distinctUntilChanged` over the poll.
    let stream = stream::unfold(
        (chain, multistream, Option::<ChainStatus>::None),
        |(chain, multistream, last)| async move {
            loop {
                let status = chain_status(&chain, &multistream);
                if last != Some(status) {
                    return Some((Ok(status), (chain, multistream, Some(status))));
                }
                tokio::time::sleep(STATUS_POLL_INTERVAL).await;
            }
        },
    );
    Box::pin(stream)
}

/// Parse the optional JSON params object from a `NativeSubscribe` payload. An
/// empty payload means "no params"; non-empty payloads must be valid JSON.
fn parse_subscribe_params(payload: &[u8]) -> Result<Option<serde_json::Value>, tonic::Status> {
    if payload.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(payload)
        .map(Some)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid subscribe params: {e}")))
}

/// Map an internal [`Capability`] to its `Describe` proto value.
fn capability_to_proto(cap: Capability) -> i32 {
    match cap {
        Capability::Rpc => Capabilities::CapCalls as i32,
        Capability::Balance => Capabilities::CapBalance as i32,
        Capability::Allowance => Capabilities::CapAllowance as i32,
    }
}

/// Build a `ChainStatus` for a chain: its aggregate availability plus the
/// number of usable upstreams. Shared by `Describe` and (later)
/// `SubscribeStatus`, mirroring the legacy `SubscribeStatus.chainStatus`.
///
/// The `UpstreamAvailability` discriminants line up 1:1 with the proto
/// `AvailabilityEnum`, so the availability maps by a direct cast.
///
/// Note: legacy counts upstreams `> UNAVAILABLE`, which — given UNAVAILABLE is
/// the worst (highest) ordinal — can never match and always yields 0. That is a
/// latent bug; we report the intended count of non-unavailable upstreams.
fn chain_status(chain: &TargetBlockchain, multistream: &Multistream) -> ChainStatus {
    use crate::upstream::availability::UpstreamAvailability;

    let availability = multistream.aggregate_availability();
    let quorum = if availability == UpstreamAvailability::Unavailable {
        0
    } else {
        multistream
            .upstreams()
            .iter()
            .filter(|u| u.availability() != UpstreamAvailability::Unavailable)
            .count() as u32
    };

    ChainStatus {
        chain: chain.id(),
        availability: availability as i32,
        quorum,
    }
}

/// Build a `ChainHead` gRPC message from a head block. `weight` carries the
/// cumulative work as a big-endian big integer (empty when unknown), matching
/// the legacy `StreamHead`.
fn chain_head(chain_id: i32, block: &BlockContainer) -> ChainHead {
    ChainHead {
        chain: chain_id,
        height: block.height,
        block_id: block.hash.to_hex(),
        timestamp: block.timestamp.as_millisecond() as u64,
        weight: if block.total_difficulty.is_zero() {
            Vec::new()
        } else {
            block.total_difficulty.to_be_bytes_trimmed_vec()
        },
        reorg: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
    use crate::upstream::Multistream;
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::quorum::{AlwaysQuorum, CallQuorum, QuorumFactory};
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::{RpcUpstream, UpstreamError};
    use emerald_api::proto::blockchain::NativeCallItem;
    use emerald_api::proto::common::ChainRef;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use tokio_stream::StreamExt;

    /// Upstream that records how many calls are in flight at once and sleeps
    /// while serving each one, so an outside observer can tell whether the
    /// handler is dispatching items concurrently.
    struct ConcurrencyProbeUpstream {
        active: AtomicU32,
        max_active: AtomicU32,
        delay: Duration,
        state: Arc<UpstreamState>,
    }

    impl ConcurrencyProbeUpstream {
        fn new(delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                active: AtomicU32::new(0),
                max_active: AtomicU32::new(0),
                delay,
                state: Arc::new(UpstreamState::new()),
            })
        }

        fn max_observed_parallel(&self) -> u32 {
            self.max_active.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for ConcurrencyProbeUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let in_flight = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(in_flight, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#).unwrap())
        }
        fn id(&self) -> &str {
            "probe"
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::Ok
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            None
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
    }

    /// Upstream stub that advertises fixed labels and capabilities, to verify
    /// `describe` surfaces per-upstream metadata through the trait (the
    /// `IdentifiedUpstream` wrapper that carries this in production is
    /// unit-tested separately).
    struct MetaUpstream {
        labels: HashMap<String, String>,
        capabilities: Vec<crate::upstream::traits::Capability>,
        state: Arc<UpstreamState>,
    }

    #[async_trait::async_trait]
    impl RpcUpstream for MetaUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            Err(UpstreamError::Transport("meta".into()))
        }
        fn id(&self) -> &str {
            "meta"
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::Ok
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            None
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
        fn labels(&self) -> &HashMap<String, String> {
            &self.labels
        }
        fn capabilities(&self) -> Vec<crate::upstream::traits::Capability> {
            self.capabilities.clone()
        }
    }

    /// Minimal `QuorumFactory` that hands out `AlwaysQuorum` for every method —
    /// the test doesn't depend on per-method routing, only on the dispatch loop.
    struct AlwaysFactory;

    impl QuorumFactory for AlwaysFactory {
        fn quorum_for(&self, _method: &RpcMethod) -> Box<dyn CallQuorum> {
            Box::new(AlwaysQuorum::new())
        }
    }

    /// Build a service whose `UpstreamManager` has a single configured chain,
    /// Ethereum, wired to `upstream` — and nothing else: no merged heads and no
    /// caches (`from_parts` with empty maps). Used by the dispatch-level handler
    /// tests, which only need one chain present; any other chain (e.g. Bitcoin)
    /// is intentionally left unconfigured so the "no upstream → unavailable"
    /// path can be exercised.
    fn eth_service_with(upstream: Arc<dyn RpcUpstream>) -> BlockchainRpcService {
        let multistream = Arc::new(Multistream::new(vec![upstream], Arc::new(AlwaysFactory)));
        let mut chains: HashMap<TargetBlockchain, Arc<Multistream>> = HashMap::new();
        chains.insert(
            TargetBlockchain::Standard(ChainRef::ChainEthereum),
            multistream,
        );
        let manager = Arc::new(UpstreamManager::from_parts(chains, HashMap::new()));
        BlockchainRpcService::new(manager)
    }

    /// Drive `native_call` to completion: invoke it and drain the reply stream.
    async fn drive_native_call(
        service: &BlockchainRpcService,
        items: Vec<NativeCallItem>,
    ) -> Vec<NativeCallReplyItem> {
        let req = NativeCallRequest {
            chain: ChainRef::ChainEthereum as i32,
            items,
            ..Default::default()
        };
        let resp = service
            .native_call(tonic::Request::new(req))
            .await
            .expect("native_call returned an error");
        let mut stream = resp.into_inner();
        let mut replies = Vec::new();
        while let Some(reply) = stream.next().await {
            replies.push(reply.expect("stream item failed"));
        }
        replies
    }

    fn make_item(id: u32) -> NativeCallItem {
        NativeCallItem {
            id,
            method: "eth_blockNumber".into(),
            payload: b"[]".to_vec(),
            ..Default::default()
        }
    }

    /// Several items in one `native_call` request must run concurrently — if
    /// they ran one-at-a-time, `max_observed_parallel` could never exceed 1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_call_items_run_in_parallel() {
        let probe = ConcurrencyProbeUpstream::new(Duration::from_millis(100));
        let service = eth_service_with(probe.clone() as Arc<dyn RpcUpstream>);

        let items: Vec<NativeCallItem> = (0..10).map(make_item).collect();
        let replies = drive_native_call(&service, items).await;

        assert_eq!(replies.len(), 10, "all items should produce a reply");
        assert!(
            replies.iter().all(|r| r.succeed),
            "all items should succeed, got: {:?}",
            replies.iter().map(|r| &r.error_message).collect::<Vec<_>>()
        );

        let observed = probe.max_observed_parallel();
        assert!(
            observed > 1,
            "expected concurrent dispatch — max in-flight calls was {observed}"
        );
    }

    /// Multiple `native_call` invocations issued concurrently against the same
    /// service must overlap. If the service somehow serialized requests (e.g.
    /// held a mutex across the call), `max_observed_parallel` would stay at 1
    /// even though the caller drove them in parallel.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_native_call_requests_overlap() {
        let probe = ConcurrencyProbeUpstream::new(Duration::from_millis(100));
        let service = Arc::new(eth_service_with(probe.clone() as Arc<dyn RpcUpstream>));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let service = service.clone();
            handles.push(tokio::spawn(async move {
                drive_native_call(service.as_ref(), vec![make_item(0)]).await
            }));
        }
        for h in handles {
            let replies = h.await.expect("task panicked");
            assert_eq!(replies.len(), 1);
            assert!(replies[0].succeed, "{}", replies[0].error_message);
        }

        let observed = probe.max_observed_parallel();
        assert!(
            observed > 1,
            "expected concurrent requests to overlap — max in-flight calls was {observed}"
        );
    }

    // ── subscribe_head ──────────────────────────────────────────────────

    #[test]
    fn chain_head_maps_block_fields() {
        let mut hash = [0u8; 32];
        hash[0] = 0xab;
        let block = BlockContainer {
            hash: crate::data::BlockId::from_bytes(hash),
            height: 12345,
            parent_hash: None,
            total_difficulty: alloy::primitives::U256::from(1024u64),
            timestamp: jiff::Timestamp::from_millisecond(1_700_000_000_000).unwrap(),
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        };
        let head = chain_head(ChainRef::ChainEthereum as i32, &block);
        assert_eq!(head.chain, ChainRef::ChainEthereum as i32);
        assert_eq!(head.height, 12345);
        assert_eq!(head.block_id, block.hash.to_hex());
        assert_eq!(head.timestamp, 1_700_000_000_000);
        assert_eq!(head.weight, vec![0x04, 0x00]); // 1024 == 0x0400
        assert_eq!(head.reorg, 0);
    }

    #[tokio::test]
    async fn subscribe_head_unknown_chain_is_invalid_argument() {
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let status = service
            .subscribe_head(tonic::Request::new(common::Chain { r#type: 999_999 }))
            .await
            .err()
            .expect("expected an error status");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn subscribe_head_without_head_stream_is_unavailable() {
        // `eth_service_with` builds the manager via `from_parts`, which has no
        // merged head streams, so a known chain still reports unavailable.
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let status = service
            .subscribe_head(tonic::Request::new(common::Chain {
                r#type: ChainRef::ChainEthereum as i32,
            }))
            .await
            .err()
            .expect("expected an error status");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    // ── native_subscribe ────────────────────────────────────────────────

    #[test]
    fn parse_subscribe_params_handles_empty_and_json() {
        assert!(parse_subscribe_params(b"").unwrap().is_none());
        assert_eq!(
            parse_subscribe_params(br#"{"address":"0x00"}"#).unwrap(),
            Some(serde_json::json!({"address": "0x00"}))
        );
        assert!(parse_subscribe_params(b"not json").is_err());
    }

    #[tokio::test]
    async fn native_subscribe_unknown_chain_is_unavailable() {
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let status = service
            .native_subscribe(tonic::Request::new(NativeSubscribeRequest {
                chain: 999_999,
                method: "newHeads".to_string(),
                payload: Vec::new(),
            }))
            .await
            .err()
            .expect("expected an error status");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    // ── describe ─────────────────────────────────────────────────────────

    /// Factory that reports a fixed supported-method list, so the `describe`
    /// test can verify the handler forwards the chain's methods. The real
    /// per-chain method tables are unit-tested in `upstream::methods`.
    struct FixedMethods(Vec<String>);

    impl QuorumFactory for FixedMethods {
        fn quorum_for(&self, _method: &RpcMethod) -> Box<dyn CallQuorum> {
            Box::new(AlwaysQuorum::new())
        }
        fn supported_methods(&self) -> Vec<String> {
            self.0.clone()
        }
    }

    /// Build a service whose Ethereum chain reports a known method set, so
    /// `supported_methods` is populated.
    fn service_with_default_methods(upstream: Arc<dyn RpcUpstream>) -> BlockchainRpcService {
        let factory: Arc<dyn QuorumFactory> = Arc::new(FixedMethods(vec![
            "eth_chainId".to_string(),
            "eth_getBalance".to_string(),
        ]));
        let multistream = Arc::new(Multistream::new(vec![upstream], factory));
        let mut chains: HashMap<TargetBlockchain, Arc<Multistream>> = HashMap::new();
        chains.insert(
            TargetBlockchain::Standard(ChainRef::ChainEthereum),
            multistream,
        );
        let manager = Arc::new(UpstreamManager::from_parts(chains, HashMap::new()));
        BlockchainRpcService::new(manager)
    }

    #[tokio::test]
    async fn describe_reports_chain_methods_and_status() {
        let service =
            service_with_default_methods(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let resp = service
            .describe(tonic::Request::new(DescribeRequest {}))
            .await
            .expect("describe failed")
            .into_inner();

        assert_eq!(resp.chains.len(), 1);
        let c = &resp.chains[0];
        assert_eq!(c.chain, ChainRef::ChainEthereum as i32);

        // Methods advertised, from the real Ethereum default table.
        assert!(c.supported_methods.contains(&"eth_getBalance".to_string()));
        assert!(c.supported_methods.contains(&"eth_chainId".to_string()));

        // NativeCall capability always present.
        assert!(c.capabilities.contains(&(Capabilities::CapCalls as i32)));

        // One node per upstream, quorum 1.
        assert_eq!(c.nodes.len(), 1);
        assert_eq!(c.nodes[0].quorum, 1);

        // Status reflects the upstream's OK availability and one in-quorum node.
        let status = c.status.as_ref().expect("status present");
        assert_eq!(status.chain, ChainRef::ChainEthereum as i32);
        assert_eq!(
            status.availability,
            crate::upstream::availability::UpstreamAvailability::Ok as i32
        );
        assert_eq!(status.quorum, 1);

        // `from_parts` builds no merged head, so the chain can't serve egress
        // subscriptions and advertises none.
        assert!(c.supported_subscriptions.is_empty());
    }

    #[tokio::test]
    async fn describe_reports_labels_and_capabilities() {
        use crate::upstream::traits::Capability;
        let upstream = Arc::new(MetaUpstream {
            labels: [("provider", "infura"), ("region", "eu")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            capabilities: vec![Capability::Rpc, Capability::Balance],
            state: Arc::new(UpstreamState::new()),
        });
        let multistream = Arc::new(Multistream::new(
            vec![upstream as Arc<dyn RpcUpstream>],
            Arc::new(AlwaysFactory),
        ));
        let mut chains: HashMap<TargetBlockchain, Arc<Multistream>> = HashMap::new();
        chains.insert(
            TargetBlockchain::Standard(ChainRef::ChainEthereum),
            multistream,
        );
        let manager = Arc::new(UpstreamManager::from_parts(chains, HashMap::new()));
        let service = BlockchainRpcService::new(manager);

        let resp = service
            .describe(tonic::Request::new(DescribeRequest {}))
            .await
            .expect("describe failed")
            .into_inner();

        let c = &resp.chains[0];
        // Node carries the upstream's labels.
        assert_eq!(c.nodes.len(), 1);
        let labels: HashMap<_, _> = c.nodes[0]
            .labels
            .iter()
            .map(|l| (l.name.as_str(), l.value.as_str()))
            .collect();
        assert_eq!(labels.get("provider"), Some(&"infura"));
        assert_eq!(labels.get("region"), Some(&"eu"));

        // Capabilities surface both RPC (CAP_CALLS) and BALANCE.
        assert!(c.capabilities.contains(&(Capabilities::CapCalls as i32)));
        assert!(c.capabilities.contains(&(Capabilities::CapBalance as i32)));
    }

    #[test]
    fn chain_status_counts_available_upstreams() {
        let multistream = Multistream::new(
            vec![ConcurrencyProbeUpstream::new(Duration::ZERO)],
            Arc::new(AlwaysFactory),
        );
        let chain = TargetBlockchain::Standard(ChainRef::ChainEthereum);
        let status = chain_status(&chain, &multistream);
        assert_eq!(status.chain, ChainRef::ChainEthereum as i32);
        assert_eq!(
            status.availability,
            crate::upstream::availability::UpstreamAvailability::Ok as i32
        );
        assert_eq!(status.quorum, 1);
    }

    // ── subscribe_status ─────────────────────────────────────────────────

    #[tokio::test]
    async fn subscribe_status_reports_configured_chain() {
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let resp = service
            .subscribe_status(tonic::Request::new(StatusRequest {
                chains: vec![ChainRef::ChainEthereum as i32],
            }))
            .await
            .expect("subscribe_status failed");
        let mut stream = resp.into_inner();

        // The current status is emitted immediately on subscribe.
        let first = stream.next().await.expect("an item").expect("status ok");
        assert_eq!(first.chain, ChainRef::ChainEthereum as i32);
        assert_eq!(first.availability, UpstreamAvailability::Ok as i32);
        assert_eq!(first.quorum, 1);
    }

    #[tokio::test]
    async fn subscribe_status_unconfigured_chain_is_unavailable_once() {
        // `eth_service_with` configures only Ethereum, so Bitcoin is unknown here.
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let resp = service
            .subscribe_status(tonic::Request::new(StatusRequest {
                chains: vec![ChainRef::ChainBitcoin as i32],
            }))
            .await
            .expect("subscribe_status failed");
        let mut stream = resp.into_inner();

        let first = stream.next().await.expect("an item").expect("status ok");
        assert_eq!(first.chain, ChainRef::ChainBitcoin as i32);
        assert_eq!(first.availability, UpstreamAvailability::Unavailable as i32);
        assert_eq!(first.quorum, 0);
        // Single-shot for an unconfigured chain: the sub-stream then completes.
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn subscribe_status_unknown_chain_id_is_unavailable() {
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let resp = service
            .subscribe_status(tonic::Request::new(StatusRequest {
                chains: vec![999_999],
            }))
            .await
            .expect("subscribe_status failed");
        let mut stream = resp.into_inner();

        // An id outside the known set still gets a definite answer carrying
        // the requested id verbatim.
        let first = stream.next().await.expect("an item").expect("status ok");
        assert_eq!(first.chain, 999_999);
        assert_eq!(first.availability, UpstreamAvailability::Unavailable as i32);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn subscribe_status_empty_request_completes_immediately() {
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let resp = service
            .subscribe_status(tonic::Request::new(StatusRequest { chains: vec![] }))
            .await
            .expect("subscribe_status failed");
        let mut stream = resp.into_inner();
        assert!(stream.next().await.is_none());
    }

    // ── estimate_fee ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn estimate_fee_unconfigured_chain_is_unavailable() {
        // `eth_service_with` configures only Ethereum, so Bitcoin is unconfigured
        // here — no upstream means no estimator.
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let status = service
            .estimate_fee(tonic::Request::new(EstimateFeeRequest {
                chain: ChainRef::ChainBitcoin as i32,
                mode: FeeEstimationMode::AvgLast as i32,
                blocks: 5,
            }))
            .await
            .err()
            .expect("expected an error status");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn estimate_fee_invalid_mode_is_rejected() {
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let status = service
            .estimate_fee(tonic::Request::new(EstimateFeeRequest {
                chain: ChainRef::ChainEthereum as i32,
                mode: FeeEstimationMode::Invalid as i32,
                blocks: 5,
            }))
            .await
            .err()
            .expect("expected an error status");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("UNSUPPORTED MODE"));
    }

    #[tokio::test]
    async fn estimate_fee_without_head_is_unavailable() {
        // The probe upstream reports no head height, so there's no window.
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let status = service
            .estimate_fee(tonic::Request::new(EstimateFeeRequest {
                chain: ChainRef::ChainEthereum as i32,
                mode: FeeEstimationMode::AvgLast as i32,
                blocks: 5,
            }))
            .await
            .err()
            .expect("expected an error status");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn native_subscribe_without_head_is_unavailable() {
        let service = eth_service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let status = service
            .native_subscribe(tonic::Request::new(NativeSubscribeRequest {
                chain: ChainRef::ChainEthereum as i32,
                method: "newHeads".to_string(),
                payload: Vec::new(),
            }))
            .await
            .err()
            .expect("expected an error status");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }
}
