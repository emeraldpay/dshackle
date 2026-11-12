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

//! A remote Dshackle for tests: serves Ethereum over the Blockchain API, with
//! an Auth API accepting [`TOKEN`].

use emerald_api::proto::auth::auth_server::{Auth, AuthServer};
use emerald_api::proto::auth::{
    AuthRequest, AuthResponse, DeleteTokenRequest, DeleteTokenResponse, IssueTokenRequest,
    IssuedTokenResponse, ListTokensRequest, ListTokensResponse, RefreshRequest, UpdateTokenRequest,
    WhoAmIRequest, WhoAmIResponse,
};
use emerald_api::proto::blockchain::blockchain_server::{Blockchain, BlockchainServer};
use emerald_api::proto::blockchain::{
    AddressAllowance, AddressAllowanceRequest, AddressBalance, BalanceRequest, ChainHead,
    ChainStatus, DescribeChain, DescribeRequest, DescribeResponse, EstimateFeeRequest,
    EstimateFeeResponse, NativeCallReplyItem, NativeCallRequest, NativeSubscribeReplyItem,
    NativeSubscribeRequest, StatusRequest, TxStatus, TxStatusRequest,
};
use emerald_api::proto::common;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_stream::wrappers::{BroadcastStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt};
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Response, Status};

/// The Auth Token the stub accepts.
pub const TOKEN: &str = "test-token";
/// The JWT the stub issues for [`TOKEN`].
pub const JWT: &str = "test-jwt";

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

pub struct StubRemote {
    /// `NativeSubscribe` streams whatever is sent here
    pub feed: broadcast::Sender<Vec<u8>>,
    /// Ends each `NativeSubscribe` stream after that many items
    end_after: Option<usize>,
    pub subscriptions: AtomicUsize,
    pub subscribe_requests: Mutex<Vec<NativeSubscribeRequest>>,
    /// The `authorization` header of each Blockchain call, empty for a call
    /// without one
    pub authorization: Mutex<Vec<String>>,
}

impl StubRemote {
    pub fn new() -> Arc<Self> {
        Self::ending_subscriptions_after(None)
    }

    pub fn ending_subscriptions_after(end_after: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            feed: broadcast::channel(16).0,
            end_after,
            subscriptions: AtomicUsize::new(0),
            subscribe_requests: Mutex::new(Vec::new()),
            authorization: Mutex::new(Vec::new()),
        })
    }

    /// Serves the stub on a free local port; returns its URL.
    pub async fn start(self: &Arc<Self>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let recorder = Arc::clone(self);
        let blockchain = InterceptedService::new(
            BlockchainServer::from_arc(Arc::clone(self)),
            move |request: Request<()>| {
                recorder.record_authorization(&request);
                Ok(request)
            },
        );
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(AuthServer::new(StubAuth))
                .add_service(blockchain)
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        url
    }

    pub fn authorization(&self) -> Vec<String> {
        self.authorization.lock().unwrap().clone()
    }

    fn record_authorization(&self, request: &Request<()>) {
        let header = request
            .metadata()
            .get("authorization")
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        self.authorization.lock().unwrap().push(header);
    }
}

#[tonic::async_trait]
impl Blockchain for StubRemote {
    type SubscribeHeadStream = BoxStream<ChainHead>;
    type SubscribeBalanceStream = BoxStream<AddressBalance>;
    type SubscribeTxStatusStream = BoxStream<TxStatus>;
    type GetBalanceStream = BoxStream<AddressBalance>;
    type GetAddressAllowanceStream = BoxStream<AddressAllowance>;
    type SubscribeAddressAllowanceStream = BoxStream<AddressAllowance>;
    type NativeCallStream = BoxStream<NativeCallReplyItem>;
    type NativeSubscribeStream = BoxStream<NativeSubscribeReplyItem>;
    type SubscribeStatusStream = BoxStream<ChainStatus>;

    async fn describe(
        &self,
        _: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        Ok(Response::new(DescribeResponse {
            chains: vec![DescribeChain {
                chain: common::ChainRef::ChainEthereum as i32,
                ..Default::default()
            }],
        }))
    }

    async fn native_call(
        &self,
        _: Request<NativeCallRequest>,
    ) -> Result<Response<Self::NativeCallStream>, Status> {
        let reply = NativeCallReplyItem {
            id: 1,
            succeed: true,
            payload: b"\"0x1\"".to_vec(),
            ..Default::default()
        };
        Ok(Response::new(Box::pin(tokio_stream::once(Ok(reply)))))
    }

    async fn subscribe_head(
        &self,
        _: Request<common::Chain>,
    ) -> Result<Response<Self::SubscribeHeadStream>, Status> {
        let head = ChainHead {
            height: 100,
            ..Default::default()
        };
        Ok(Response::new(Box::pin(tokio_stream::once(Ok(head)))))
    }

    async fn native_subscribe(
        &self,
        request: Request<NativeSubscribeRequest>,
    ) -> Result<Response<Self::NativeSubscribeStream>, Status> {
        self.subscriptions.fetch_add(1, Ordering::SeqCst);
        self.subscribe_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        let stream = BroadcastStream::new(self.feed.subscribe())
            .filter_map(|item| item.ok())
            .map(|payload| Ok(NativeSubscribeReplyItem { payload }));
        Ok(Response::new(match self.end_after {
            Some(n) => Box::pin(stream.take(n)),
            None => Box::pin(stream),
        }))
    }

    async fn subscribe_balance(
        &self,
        _: Request<BalanceRequest>,
    ) -> Result<Response<Self::SubscribeBalanceStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn subscribe_tx_status(
        &self,
        _: Request<TxStatusRequest>,
    ) -> Result<Response<Self::SubscribeTxStatusStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn get_balance(
        &self,
        _: Request<BalanceRequest>,
    ) -> Result<Response<Self::GetBalanceStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn get_address_allowance(
        &self,
        _: Request<AddressAllowanceRequest>,
    ) -> Result<Response<Self::GetAddressAllowanceStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn subscribe_address_allowance(
        &self,
        _: Request<AddressAllowanceRequest>,
    ) -> Result<Response<Self::SubscribeAddressAllowanceStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn estimate_fee(
        &self,
        _: Request<EstimateFeeRequest>,
    ) -> Result<Response<EstimateFeeResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn subscribe_status(
        &self,
        _: Request<StatusRequest>,
    ) -> Result<Response<Self::SubscribeStatusStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
}

/// Issues a long-living [`JWT`] for [`TOKEN`], denies any other token.
struct StubAuth;

#[tonic::async_trait]
impl Auth for StubAuth {
    async fn authenticate(
        &self,
        request: Request<AuthRequest>,
    ) -> Result<Response<AuthResponse>, Status> {
        if request.into_inner().auth_secret != TOKEN {
            return Ok(Response::new(AuthResponse {
                status: 401,
                deny_message: "unknown token".into(),
                ..Default::default()
            }));
        }
        let expires_at = jiff::Timestamp::now() + jiff::SignedDuration::from_hours(1);
        Ok(Response::new(AuthResponse {
            status: 0,
            access_token: JWT.into(),
            refresh_token: "test-refresh".into(),
            expires_at: expires_at.as_millisecond() as u64,
            ..Default::default()
        }))
    }
    async fn refresh(&self, _: Request<RefreshRequest>) -> Result<Response<AuthResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn issue_token(
        &self,
        _: Request<IssueTokenRequest>,
    ) -> Result<Response<IssuedTokenResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn update_token(
        &self,
        _: Request<UpdateTokenRequest>,
    ) -> Result<Response<IssuedTokenResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn who_am_i(
        &self,
        _: Request<WhoAmIRequest>,
    ) -> Result<Response<WhoAmIResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn list_tokens(
        &self,
        _: Request<ListTokensRequest>,
    ) -> Result<Response<ListTokensResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn delete_token(
        &self,
        _: Request<DeleteTokenRequest>,
    ) -> Result<Response<DeleteTokenResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
}
