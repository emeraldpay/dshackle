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
use crate::rpc::native_call;
use crate::upstream::UpstreamManager;
use emerald_api::proto::blockchain::blockchain_server::Blockchain;
use emerald_api::proto::blockchain::*;
use emerald_api::proto::common;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;

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
        tracing::trace!(chain = req.chain, items = req.items.len(), "native_call request");

        let chain = TargetBlockchain::try_from(req.chain).map_err(|id| {
            tracing::trace!(chain = id, "unknown chain id");
            tonic::Status::invalid_argument(format!("unknown chain id {id}"))
        })?;

        let upstream = self
            .upstreams
            .get(&chain)
            .ok_or_else(|| {
                tracing::trace!(%chain, "no upstream for chain");
                tonic::Status::unavailable(format!("no upstream available for chain {chain}"))
            })?
            .clone();

        // Process each call item sequentially and collect results.
        // Future improvement: process concurrently with FuturesOrdered.
        let mut results = Vec::with_capacity(req.items.len());
        for item in &req.items {
            let reply = native_call::execute_native_call(upstream.as_ref(), item).await;
            results.push(Ok(reply));
        }

        tracing::trace!(chain = req.chain, items = results.len(), "native_call done");
        let stream = tokio_stream::iter(results);
        Ok(tonic::Response::new(Box::pin(stream)))
    }

    // ── All other methods are not yet implemented ────────────────────────

    async fn subscribe_head(
        &self,
        _request: tonic::Request<common::Chain>,
    ) -> Result<tonic::Response<Self::SubscribeHeadStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("subscribe_head not yet implemented"))
    }

    async fn subscribe_balance(
        &self,
        _request: tonic::Request<BalanceRequest>,
    ) -> Result<tonic::Response<Self::SubscribeBalanceStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("subscribe_balance not yet implemented"))
    }

    async fn subscribe_tx_status(
        &self,
        _request: tonic::Request<TxStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTxStatusStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("subscribe_tx_status not yet implemented"))
    }

    async fn get_balance(
        &self,
        _request: tonic::Request<BalanceRequest>,
    ) -> Result<tonic::Response<Self::GetBalanceStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_balance not yet implemented"))
    }

    async fn get_address_allowance(
        &self,
        _request: tonic::Request<AddressAllowanceRequest>,
    ) -> Result<tonic::Response<Self::GetAddressAllowanceStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_address_allowance not yet implemented"))
    }

    async fn subscribe_address_allowance(
        &self,
        _request: tonic::Request<AddressAllowanceRequest>,
    ) -> Result<tonic::Response<Self::SubscribeAddressAllowanceStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("subscribe_address_allowance not yet implemented"))
    }

    async fn estimate_fee(
        &self,
        _request: tonic::Request<EstimateFeeRequest>,
    ) -> Result<tonic::Response<EstimateFeeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("estimate_fee not yet implemented"))
    }

    async fn native_subscribe(
        &self,
        _request: tonic::Request<NativeSubscribeRequest>,
    ) -> Result<tonic::Response<Self::NativeSubscribeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("native_subscribe not yet implemented"))
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
        Err(tonic::Status::unimplemented("subscribe_status not yet implemented"))
    }
}
