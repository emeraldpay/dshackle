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
//! Currently only `native_call` is implemented; all other methods return
//! `UNIMPLEMENTED` status.

use crate::blockchain::TargetBlockchain;
use crate::data::BlockContainer;
use crate::rpc::native_call;
use crate::upstream::UpstreamManager;
use emerald_api::proto::blockchain::blockchain_server::Blockchain;
use emerald_api::proto::blockchain::*;
use emerald_api::proto::common;
use futures::stream::{FuturesUnordered, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tokio_stream::wrappers::BroadcastStream;

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
        _request: tonic::Request<EstimateFeeRequest>,
    ) -> Result<tonic::Response<EstimateFeeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "estimate_fee not yet implemented",
        ))
    }

    async fn native_subscribe(
        &self,
        _request: tonic::Request<NativeSubscribeRequest>,
    ) -> Result<tonic::Response<Self::NativeSubscribeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "native_subscribe not yet implemented",
        ))
    }

    async fn describe(
        &self,
        _request: tonic::Request<DescribeRequest>,
    ) -> Result<tonic::Response<DescribeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("describe not yet implemented"))
    }

    async fn subscribe_status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeStatusStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "subscribe_status not yet implemented",
        ))
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

    /// Minimal `QuorumFactory` that hands out `AlwaysQuorum` for every method —
    /// the test doesn't depend on per-method routing, only on the dispatch loop.
    struct AlwaysFactory;

    impl QuorumFactory for AlwaysFactory {
        fn quorum_for(&self, _method: &RpcMethod) -> Box<dyn CallQuorum> {
            Box::new(AlwaysQuorum::new())
        }
    }

    fn service_with(upstream: Arc<dyn RpcUpstream>) -> BlockchainRpcService {
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
        let service = service_with(probe.clone() as Arc<dyn RpcUpstream>);

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
        let service = Arc::new(service_with(probe.clone() as Arc<dyn RpcUpstream>));

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
        let service = service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let status = service
            .subscribe_head(tonic::Request::new(common::Chain { r#type: 999_999 }))
            .await
            .err()
            .expect("expected an error status");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn subscribe_head_without_head_stream_is_unavailable() {
        // `service_with` builds the manager via `from_parts`, which has no
        // merged head streams, so a known chain still reports unavailable.
        let service = service_with(ConcurrencyProbeUpstream::new(Duration::ZERO));
        let status = service
            .subscribe_head(tonic::Request::new(common::Chain {
                r#type: ChainRef::ChainEthereum as i32,
            }))
            .await
            .err()
            .expect("expected an error status");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }
}
