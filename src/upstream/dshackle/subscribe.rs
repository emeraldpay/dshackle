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

//! Subscription topics relayed from a remote Dshackle via `NativeSubscribe`.
//!
//! The remote lists the topics it can serve in `Describe`
//! `supportedSubscriptions`; each becomes available to local clients by
//! relaying the remote's `NativeSubscribe` stream, with the payload passed
//! through as raw bytes. Ports the legacy
//! `EthereumDshackleIngressSubscription` / `BitcoinDshackleIngressSubscription`.
//!
//! Like their `SharedFluxHolder` + `DurableFlux` machinery, a topic's remote
//! stream is shared and demand-driven: the first local subscriber starts it,
//! everyone shares one stream, it reconnects with backoff while subscribers
//! remain, and it is torn down once the last one is gone.

use crate::upstream::egress::{EgressError, EgressSubscription, SubscriptionStream};
use crate::upstream::fanout::SharedFanout;
use crate::upstream::id::UpstreamId;
use emerald_api::proto::blockchain::NativeSubscribeRequest;
use emerald_api::proto::blockchain::blockchain_client::BlockchainClient;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tonic::transport::Channel;

/// Reconnect backoff for a lost remote stream, matching the legacy
/// `DurableFlux` settings.
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
const RETRY_BACKOFF: f64 = 1.5;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Server-pushed subscriptions relayed from one remote Dshackle for a single
/// chain: whatever topics the remote advertised over `Describe`.
pub struct DshackleEgress {
    upstream: UpstreamId,
    chain_ref: i32,
    client: BlockchainClient<Channel>,
    topics: Vec<String>,
    /// One relay per param-less topic, so all its local subscribers share a
    /// single remote stream. Subscriptions with params don't share — the
    /// params make the remote stream subscriber-specific.
    shared: Mutex<HashMap<String, Arc<TopicRelay>>>,
}

impl DshackleEgress {
    /// `topics` is the remote's advertised `supportedSubscriptions` list for
    /// this chain; anything else is rejected without calling the remote.
    pub fn new(
        upstream: UpstreamId,
        chain_ref: i32,
        client: BlockchainClient<Channel>,
        topics: Vec<String>,
    ) -> Self {
        Self {
            upstream,
            chain_ref,
            client,
            topics,
            shared: Mutex::new(HashMap::new()),
        }
    }

    fn relay_for(&self, method: &str, params: Option<serde_json::Value>) -> Arc<TopicRelay> {
        let payload = match &params {
            // The payload carries the subscription params as JSON — the same
            // format the serving side parses them from.
            Some(value) => serde_json::to_vec(value).expect("JSON value always serializes"),
            None => Vec::new(),
        };
        let request = NativeSubscribeRequest {
            chain: self.chain_ref,
            method: method.to_string(),
            payload,
        };
        if params.is_some() {
            return Arc::new(TopicRelay::new(
                self.upstream.clone(),
                request,
                self.client.clone(),
            ));
        }
        let mut shared = self.shared.lock().expect("dshackle egress lock poisoned");
        Arc::clone(shared.entry(method.to_string()).or_insert_with(|| {
            Arc::new(TopicRelay::new(
                self.upstream.clone(),
                request,
                self.client.clone(),
            ))
        }))
    }
}

impl EgressSubscription for DshackleEgress {
    fn subscribe(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<SubscriptionStream, EgressError> {
        if !self.topics.iter().any(|t| t == method) {
            return Err(EgressError::UnsupportedMethod(method.to_string()));
        }
        let relay = self.relay_for(method, params);
        let stream = BroadcastStream::new(relay.subscribe())
            // A lagged subscriber just skips ahead; delivery is best-effort.
            .filter_map(|item| item.ok());
        Ok(Box::pin(stream))
    }

    fn available_topics(&self) -> Vec<String> {
        self.topics.clone()
    }
}

/// One relayed subscription: a single remote `NativeSubscribe` stream fanned
/// out to any number of local subscribers.
///
/// Holds no connection until someone subscribes; the stream reconnects with
/// backoff while subscribers remain and its task shuts down as soon as the
/// last one is gone.
struct TopicRelay {
    upstream: UpstreamId,
    request: NativeSubscribeRequest,
    client: BlockchainClient<Channel>,
    fanout: SharedFanout<Vec<u8>>,
}

impl TopicRelay {
    fn new(
        upstream: UpstreamId,
        request: NativeSubscribeRequest,
        client: BlockchainClient<Channel>,
    ) -> Self {
        Self {
            upstream,
            request,
            client,
            fanout: SharedFanout::new(),
        }
    }

    /// Attach to the shared stream, starting it if this is the first
    /// subscriber.
    fn subscribe(self: &Arc<Self>) -> broadcast::Receiver<Vec<u8>> {
        self.fanout.subscribe(|tx| {
            tokio::spawn(Arc::clone(self).run(tx));
        })
    }

    async fn run(self: Arc<Self>, tx: broadcast::Sender<Vec<u8>>) {
        let mut delay = INITIAL_RETRY_DELAY;
        loop {
            let mut client = self.client.clone();
            match client.native_subscribe(self.request.clone()).await {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    loop {
                        let message = tokio::select! {
                            message = stream.message() => message,
                            // All receivers gone — unless one is attaching
                            // this instant, drop the remote stream right away
                            // instead of streaming for nobody.
                            _ = tx.closed() => {
                                if self.finished(&tx) {
                                    return;
                                }
                                continue;
                            }
                        };
                        match message {
                            Ok(Some(item)) => {
                                // The backoff resets only once a message
                                // actually arrives (legacy `DurableFlux`): a
                                // remote that accepts the stream and then
                                // immediately fails it must keep backing off,
                                // not reconnect at the initial rate forever.
                                delay = INITIAL_RETRY_DELAY;
                                if tx.send(item.payload).is_err() && self.finished(&tx) {
                                    return;
                                }
                            }
                            Ok(None) => {
                                tracing::warn!(
                                    upstream = %self.upstream,
                                    topic = %self.request.method,
                                    "relayed subscription closed, will re-subscribe"
                                );
                                break;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    upstream = %self.upstream,
                                    topic = %self.request.method,
                                    error = %e,
                                    "relayed subscription error, will re-subscribe"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        upstream = %self.upstream,
                        topic = %self.request.method,
                        error = %e,
                        "native_subscribe failed, retrying"
                    );
                }
            }
            if self.finished(&tx) {
                return;
            }
            // Wake early when the last subscriber leaves mid-backoff, so the
            // relay never reconnects for an audience that is already gone.
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = tx.closed() => {}
            }
            if self.finished(&tx) {
                return;
            }
            delay = delay.mul_f64(RETRY_BACKOFF).min(MAX_RETRY_DELAY);
        }
    }

    /// Whether the relay should stop: no subscribers remain.
    fn finished(&self, tx: &broadcast::Sender<Vec<u8>>) -> bool {
        if !self.fanout.finish_if_deserted(tx) {
            return false;
        }
        tracing::debug!(
            upstream = %self.upstream,
            topic = %self.request.method,
            "no subscribers left, closing the relayed subscription"
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use emerald_api::proto::blockchain::blockchain_server::{Blockchain, BlockchainServer};
    use emerald_api::proto::blockchain::{
        AddressAllowance, AddressAllowanceRequest, AddressBalance, BalanceRequest, ChainHead,
        ChainStatus, DescribeRequest, DescribeResponse, EstimateFeeRequest, EstimateFeeResponse,
        NativeCallReplyItem, NativeCallRequest, NativeSubscribeReplyItem, StatusRequest, TxStatus,
        TxStatusRequest,
    };
    use emerald_api::proto::common;
    use serde_json::json;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_stream::Stream;
    use tokio_stream::wrappers::TcpListenerStream;

    type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send>>;

    /// A remote Dshackle stub: `NativeSubscribe` records each request and
    /// streams whatever the test feeds, ending the stream after `end_after`
    /// items when set. Everything else is unimplemented.
    struct StubRemote {
        connections: AtomicUsize,
        requests: Mutex<Vec<NativeSubscribeRequest>>,
        feed: broadcast::Sender<Vec<u8>>,
        end_after: Option<usize>,
    }

    impl StubRemote {
        fn new(end_after: Option<usize>) -> Arc<Self> {
            Arc::new(Self {
                connections: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
                feed: broadcast::channel(16).0,
                end_after,
            })
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

        async fn native_subscribe(
            &self,
            request: tonic::Request<NativeSubscribeRequest>,
        ) -> Result<tonic::Response<Self::NativeSubscribeStream>, tonic::Status> {
            self.connections.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.into_inner());
            let stream = BroadcastStream::new(self.feed.subscribe())
                .filter_map(|item| item.ok())
                .map(|payload| Ok(NativeSubscribeReplyItem { payload }));
            Ok(tonic::Response::new(match self.end_after {
                Some(n) => Box::pin(stream.take(n)),
                None => Box::pin(stream),
            }))
        }

        async fn native_call(
            &self,
            _: tonic::Request<NativeCallRequest>,
        ) -> Result<tonic::Response<Self::NativeCallStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn subscribe_head(
            &self,
            _: tonic::Request<common::Chain>,
        ) -> Result<tonic::Response<Self::SubscribeHeadStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn subscribe_balance(
            &self,
            _: tonic::Request<BalanceRequest>,
        ) -> Result<tonic::Response<Self::SubscribeBalanceStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn subscribe_tx_status(
            &self,
            _: tonic::Request<TxStatusRequest>,
        ) -> Result<tonic::Response<Self::SubscribeTxStatusStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn get_balance(
            &self,
            _: tonic::Request<BalanceRequest>,
        ) -> Result<tonic::Response<Self::GetBalanceStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn get_address_allowance(
            &self,
            _: tonic::Request<AddressAllowanceRequest>,
        ) -> Result<tonic::Response<Self::GetAddressAllowanceStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn subscribe_address_allowance(
            &self,
            _: tonic::Request<AddressAllowanceRequest>,
        ) -> Result<tonic::Response<Self::SubscribeAddressAllowanceStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn estimate_fee(
            &self,
            _: tonic::Request<EstimateFeeRequest>,
        ) -> Result<tonic::Response<EstimateFeeResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn describe(
            &self,
            _: tonic::Request<DescribeRequest>,
        ) -> Result<tonic::Response<DescribeResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn subscribe_status(
            &self,
            _: tonic::Request<StatusRequest>,
        ) -> Result<tonic::Response<Self::SubscribeStatusStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
    }

    async fn start_remote(stub: Arc<StubRemote>) -> BlockchainClient<Channel> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BlockchainServer::from_arc(stub))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy();
        BlockchainClient::new(channel)
    }

    fn egress(client: BlockchainClient<Channel>, topics: &[&str]) -> DshackleEgress {
        DshackleEgress::new(
            "remote".parse().unwrap(),
            1,
            client,
            topics.iter().map(|t| t.to_string()).collect(),
        )
    }

    /// Feed `payload` until the relay's remote stream is up and delivers one
    /// message to `stream` — the relay connects asynchronously, so early
    /// feeds land before anyone listens.
    async fn feed_until_received(
        stub: &StubRemote,
        stream: &mut SubscriptionStream,
        payload: &[u8],
    ) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let _ = stub.feed.send(payload.to_vec());
                tokio::select! {
                    item = stream.next() => return item.expect("stream ended"),
                    _ = tokio::time::sleep(Duration::from_millis(20)) => continue,
                }
            }
        })
        .await
        .expect("no message received")
    }

    #[tokio::test]
    async fn unadvertised_topic_is_rejected_without_calling_the_remote() {
        let stub = StubRemote::new(None);
        let client = start_remote(Arc::clone(&stub)).await;
        let egress = egress(client, &["hashtx"]);

        assert!(matches!(
            egress.subscribe("rawtx", None),
            Err(EgressError::UnsupportedMethod(m)) if m == "rawtx"
        ));
        assert_eq!(stub.connections.load(Ordering::SeqCst), 0);
        assert_eq!(egress.available_topics(), vec!["hashtx".to_string()]);
    }

    #[tokio::test]
    async fn relays_remote_payloads() {
        let stub = StubRemote::new(None);
        let client = start_remote(Arc::clone(&stub)).await;
        let egress = egress(client, &["hashtx"]);

        let mut stream = egress.subscribe("hashtx", None).unwrap();
        let payload = vec![0x42u8; 32];
        let received = feed_until_received(&stub, &mut stream, &payload).await;
        assert_eq!(received, payload);

        let requests = stub.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].chain, 1);
        assert_eq!(requests[0].method, "hashtx");
        assert!(requests[0].payload.is_empty());
    }

    #[tokio::test]
    async fn subscribers_share_one_remote_stream() {
        let stub = StubRemote::new(None);
        let client = start_remote(Arc::clone(&stub)).await;
        let egress = egress(client, &["hashtx"]);

        let mut first = egress.subscribe("hashtx", None).unwrap();
        let mut second = egress.subscribe("hashtx", None).unwrap();

        let payload = vec![0x11u8; 8];
        let received = feed_until_received(&stub, &mut first, &payload).await;
        assert_eq!(received, payload);
        let received = tokio::time::timeout(Duration::from_secs(5), second.next())
            .await
            .expect("second subscriber received nothing")
            .unwrap();
        assert_eq!(received, payload);

        assert_eq!(stub.connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn params_open_a_dedicated_stream_and_forward_the_payload() {
        let stub = StubRemote::new(None);
        let client = start_remote(Arc::clone(&stub)).await;
        let egress = egress(client, &["logs"]);

        let mut plain = egress.subscribe("logs", None).unwrap();
        let params = json!({"address": "0xabc"});
        let mut filtered = egress.subscribe("logs", Some(params.clone())).unwrap();

        // Fed separately: the two remote streams connect independently, so
        // each is driven until its own delivery is seen.
        let payload = vec![0x33u8; 4];
        feed_until_received(&stub, &mut plain, &payload).await;
        let received = feed_until_received(&stub, &mut filtered, &payload).await;
        assert_eq!(received, payload);

        assert_eq!(stub.connections.load(Ordering::SeqCst), 2);
        let requests = stub.requests.lock().unwrap();
        let payloads: Vec<serde_json::Value> = requests
            .iter()
            .filter(|r| !r.payload.is_empty())
            .map(|r| serde_json::from_slice(&r.payload).unwrap())
            .collect();
        assert_eq!(payloads, vec![params]);
    }

    #[tokio::test]
    async fn tears_down_promptly_after_the_last_subscriber_leaves() {
        let stub = StubRemote::new(None);
        let client = start_remote(Arc::clone(&stub)).await;
        let egress = egress(client, &["hashtx"]);

        let mut stream = egress.subscribe("hashtx", None).unwrap();
        feed_until_received(&stub, &mut stream, &[0x01]).await;
        let relay = Arc::clone(egress.shared.lock().unwrap().get("hashtx").unwrap());
        assert_eq!(relay.fanout.subscribers(), 1);

        // No traffic needed: the drop alone must stop the relay (via the
        // broadcast `closed()` signal), not the next message or an idle tick.
        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), async {
            while relay.fanout.is_live() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("relay did not stop after the last subscriber left");
    }

    #[tokio::test]
    async fn reconnects_after_the_remote_stream_ends() {
        // Each remote stream ends after one message; a subscriber still gets
        // a second one because the relay re-subscribes with backoff.
        let stub = StubRemote::new(Some(1));
        let client = start_remote(Arc::clone(&stub)).await;
        let egress = egress(client, &["hashtx"]);

        let mut stream = egress.subscribe("hashtx", None).unwrap();
        feed_until_received(&stub, &mut stream, &[0x01]).await;
        feed_until_received(&stub, &mut stream, &[0x02]).await;

        assert!(stub.connections.load(Ordering::SeqCst) >= 2);
    }
}
