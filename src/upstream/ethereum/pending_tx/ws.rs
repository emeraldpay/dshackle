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

//! Pending transactions announced by a node over its WebSocket connection.
//! Ports the legacy `WebsocketPendingTxes`.

use super::{PendingTxSource, PendingTxStream, tx_id_from_message};
use crate::data::TxId;
use crate::upstream::egress::METHOD_PENDING_TXES;
use crate::upstream::ethereum::EthereumWsUpstream;
use crate::upstream::ethereum::ws_conn::WsSubscription;
use crate::upstream::fanout::SharedFanout;
use crate::upstream::traits::RpcUpstream;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

/// Re-subscribe backoff, matching the legacy `DefaultPendingTxesSource`.
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
const RETRY_BACKOFF: f64 = 1.5;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Re-subscribe when the node announced nothing for this long (legacy uses
/// the same 60 seconds). A mempool this quiet is unusual on a public network,
/// while a subscription the node silently stopped serving looks exactly like
/// it — and gives no other signal.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// The `newPendingTransactions` subscription of one node, shared by everyone
/// listening to it.
pub struct WsPendingTxes {
    upstream: Arc<EthereumWsUpstream>,
    fanout: Arc<SharedFanout<TxId>>,
}

impl WsPendingTxes {
    /// Nothing is subscribed on the node until the first listener attaches.
    pub fn new(upstream: Arc<EthereumWsUpstream>) -> Self {
        Self {
            upstream,
            fanout: Arc::new(SharedFanout::new()),
        }
    }
}

impl PendingTxSource for WsPendingTxes {
    fn subscribe(&self) -> PendingTxStream {
        let receiver = self.fanout.subscribe(|listeners| {
            let feed = NodeFeed {
                upstream: Arc::clone(&self.upstream),
                fanout: Arc::clone(&self.fanout),
                listeners,
            };
            tokio::spawn(feed.run());
        });
        // A lagged listener just skips ahead; delivery is best-effort.
        Box::pin(BroadcastStream::new(receiver).filter_map(|item| item.ok()))
    }
}

/// Why a node subscription stopped being forwarded.
enum SubscriptionEnd {
    /// The last listener is gone, and the feed with it.
    Deserted,
    /// The connection carrying the subscription dropped.
    Lost,
    /// The node announced nothing for [`IDLE_TIMEOUT`].
    Quiet,
}

/// The task feeding a node's announcements to the listeners. Lives from the
/// first listener to the last, re-subscribing on the node whenever the
/// subscription is lost in between.
struct NodeFeed {
    upstream: Arc<EthereumWsUpstream>,
    fanout: Arc<SharedFanout<TxId>>,
    listeners: broadcast::Sender<TxId>,
}

impl NodeFeed {
    async fn run(self) {
        let mut delay = INITIAL_RETRY_DELAY;
        loop {
            // Deliberately not raced against the listeners leaving: abandoning
            // the request half-way would leave a subscription on the node that
            // nobody holds, and so nobody can cancel.
            match self.upstream.subscribe(METHOD_PENDING_TXES).await {
                Ok(subscription) => match self.forward(subscription, &mut delay).await {
                    SubscriptionEnd::Deserted => return,
                    // Nothing failed, so there is nothing to back off from:
                    // a delay here is only time spent unsubscribed.
                    SubscriptionEnd::Quiet => continue,
                    SubscriptionEnd::Lost => {}
                },
                Err(e) => {
                    tracing::debug!(
                        upstream = %self.upstream.id(),
                        error = %e,
                        "failed to subscribe to pending transactions, retrying"
                    );
                }
            }
            if self.deserted() {
                return;
            }
            // Wake early when the last listener leaves mid-backoff, so the
            // feed never re-subscribes for an audience that is already gone.
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = self.listeners.closed() => {}
            }
            if self.deserted() {
                return;
            }
            delay = delay.mul_f64(RETRY_BACKOFF).min(MAX_RETRY_DELAY);
        }
    }

    /// Passes announcements on for as long as the subscription lasts.
    /// Returning drops the subscription, which cancels it on the node.
    async fn forward(
        &self,
        mut subscription: WsSubscription,
        delay: &mut Duration,
    ) -> SubscriptionEnd {
        loop {
            let announced = tokio::select! {
                announced = tokio::time::timeout(IDLE_TIMEOUT, subscription.recv()) => announced,
                _ = self.listeners.closed() => {
                    if self.deserted() {
                        return SubscriptionEnd::Deserted;
                    }
                    continue;
                }
            };
            match announced {
                Ok(Some(message)) => {
                    // The backoff resets only once the node actually delivers
                    // (legacy `DurableFlux`): a node that accepts the
                    // subscription and then drops it right away must keep
                    // backing off.
                    *delay = INITIAL_RETRY_DELAY;
                    match tx_id_from_message(message.get().as_bytes()) {
                        Some(tx) => {
                            if self.listeners.send(tx).is_err() && self.deserted() {
                                return SubscriptionEnd::Deserted;
                            }
                        }
                        None => {
                            tracing::debug!(
                                upstream = %self.upstream.id(),
                                "ignoring an invalid pending transaction announcement"
                            );
                        }
                    }
                }
                Ok(None) => {
                    tracing::debug!(
                        upstream = %self.upstream.id(),
                        "pending transactions subscription closed, will re-subscribe"
                    );
                    return SubscriptionEnd::Lost;
                }
                Err(_idle) => {
                    tracing::debug!(
                        upstream = %self.upstream.id(),
                        "no pending transactions announced, re-subscribing"
                    );
                    return SubscriptionEnd::Quiet;
                }
            }
        }
    }

    /// Whether the feed should stop: no listeners remain.
    fn deserted(&self) -> bool {
        self.fanout.finish_if_deserted(&self.listeners)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::ethereum::WsTarget;
    use crate::upstream::ethereum::ws_stub::StubNode;
    use crate::upstream::state::UpstreamState;
    use serde_json::json;

    const TX_1: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    const TX_2: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";

    fn source_for(node: &StubNode) -> WsPendingTxes {
        WsPendingTxes::new(Arc::new(EthereumWsUpstream::new(
            crate::upstream::id::stub_id().clone(),
            WsTarget {
                url: node.url(),
                origin: None,
                basic_auth: None,
                call_timeout: Duration::from_secs(1),
                frame_size: None,
                msg_size: None,
            },
            1,
            Arc::new(UpstreamState::new()),
        )))
    }

    async fn next_hash(stream: &mut PendingTxStream) -> String {
        tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a pending transaction in time")
            .expect("an open stream")
            .to_hex_prefixed()
    }

    #[tokio::test]
    async fn listeners_share_one_node_subscription() {
        let node = StubNode::start().await;
        let source = source_for(&node);

        let mut first = source.subscribe();
        let mut second = source.subscribe();
        assert_eq!(
            node.requests("eth_subscribe", 1).await,
            vec![json!(["newPendingTransactions"])]
        );

        node.notify(json!(TX_1));
        assert_eq!(next_hash(&mut first).await, TX_1);
        assert_eq!(next_hash(&mut second).await, TX_1);
        assert_eq!(node.requests("eth_subscribe", 1).await.len(), 1);
    }

    #[tokio::test]
    async fn skips_announcements_that_are_not_hashes() {
        let node = StubNode::start().await;
        let source = source_for(&node);
        let mut listener = source.subscribe();
        node.requests("eth_subscribe", 1).await;

        node.notify(json!({"hash": TX_1}));
        node.notify(json!(TX_2));
        assert_eq!(next_hash(&mut listener).await, TX_2);
    }

    #[tokio::test]
    async fn cancels_node_subscription_after_last_listener() {
        let node = StubNode::start().await;
        let source = source_for(&node);

        let first = source.subscribe();
        let second = source.subscribe();
        node.requests("eth_subscribe", 1).await;

        drop(first);
        drop(second);
        assert_eq!(
            node.requests("eth_unsubscribe", 1).await,
            vec![json!(["0x1"])]
        );

        // Nothing is kept from the cancelled subscription: the next listener
        // starts a new one.
        let _again = source.subscribe();
        node.requests("eth_subscribe", 2).await;
    }

    #[tokio::test]
    async fn resubscribes_after_connection_loss() {
        let node = StubNode::start().await;
        let source = source_for(&node);
        let mut listener = source.subscribe();
        node.requests("eth_subscribe", 1).await;

        node.disconnect();
        node.requests("eth_subscribe", 2).await;

        node.notify(json!(TX_1));
        assert_eq!(next_hash(&mut listener).await, TX_1);
    }
}
