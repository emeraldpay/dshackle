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

//! Bitcoin ZMQ topics re-exposed as egress subscriptions.
//!
//! bitcoind has no subscription mechanism in its JSON-RPC API; what it offers
//! instead are ZMQ notification topics. The topics listed under
//! `zeromq.topics` in an upstream's config are served to gRPC
//! `NativeSubscribe` clients with the notification payload passed through as
//! raw bytes, exactly as the legacy `BitcoinZmqSubscriptionSource` →
//! `NativeSubscribe.convertToProto` chain did. gRPC only: the WS proxy's
//! `eth_subscribe` is an Ethereum protocol feature and deliberately does not
//! carry these topics (legacy leaked them there unintentionally).
//!
//! A topic's upstream connection is shared and demand-driven, like the legacy
//! `SharedFluxHolder`: the first subscriber starts it, everyone shares one SUB
//! socket, and it is torn down once the last subscriber is gone — important
//! for `rawtx`/`rawblock`, where an idle connection would otherwise keep
//! streaming the node's full transaction firehose for nobody.

use crate::upstream::bitcoin::zmq::{ZmqEvent, ZmqFlow, run_subscription};
use crate::upstream::egress::{EgressError, EgressSubscription, SubscriptionStream};
use crate::upstream::id::UpstreamId;
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

/// How many notifications a subscriber may fall behind before it starts losing
/// them. Delivery is best-effort, like the legacy `directBestEffort` sink: a
/// slow consumer skips ahead rather than back-pressuring the node connection.
const TOPIC_CHANNEL_CAPACITY: usize = 256;

/// The ZMQ notification topics bitcoind can publish, as configurable under
/// `zeromq.topics`. Ports the legacy `BitcoinZmqTopic`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitcoinZmqTopic {
    Hashblock,
    Hashtx,
    Rawblock,
    Rawtx,
}

impl BitcoinZmqTopic {
    const ALL: [BitcoinZmqTopic; 4] = [
        BitcoinZmqTopic::Hashblock,
        BitcoinZmqTopic::Hashtx,
        BitcoinZmqTopic::Rawblock,
        BitcoinZmqTopic::Rawtx,
    ];

    /// The topic string as bitcoind publishes it and as clients subscribe to it.
    pub fn id(&self) -> &'static str {
        match self {
            BitcoinZmqTopic::Hashblock => "hashblock",
            BitcoinZmqTopic::Hashtx => "hashtx",
            BitcoinZmqTopic::Rawblock => "rawblock",
            BitcoinZmqTopic::Rawtx => "rawtx",
        }
    }

    pub fn find_by_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.id() == id)
    }
}

/// One upstream's shared stream of a single ZMQ topic.
///
/// Holds no connection until someone subscribes; all subscribers share one
/// connection, and its task shuts down after the last one is gone (checked on
/// each delivery and on the idle rebuild tick).
pub struct ZmqTopicStream {
    endpoint: String,
    topic: BitcoinZmqTopic,
    upstream: UpstreamId,
    /// The live connection's broadcast side, present only while the
    /// connection task runs.
    sender: Mutex<Option<broadcast::Sender<Bytes>>>,
}

impl ZmqTopicStream {
    pub fn new(host: &str, port: u16, topic: BitcoinZmqTopic, upstream: UpstreamId) -> Self {
        Self {
            endpoint: format!("tcp://{host}:{port}"),
            topic,
            upstream,
            sender: Mutex::new(None),
        }
    }

    pub fn topic(&self) -> BitcoinZmqTopic {
        self.topic
    }

    /// Attach to the shared connection, starting it if this is the first
    /// subscriber.
    pub fn subscribe(self: &Arc<Self>) -> broadcast::Receiver<Bytes> {
        let mut sender = self.sender.lock().expect("zmq topic lock poisoned");
        if let Some(tx) = sender.as_ref() {
            return tx.subscribe();
        }
        let (tx, rx) = broadcast::channel(TOPIC_CHANNEL_CAPACITY);
        *sender = Some(tx.clone());

        let this = Arc::clone(self);
        tokio::spawn(run_subscription(
            self.endpoint.clone(),
            self.topic.id().to_string(),
            self.upstream.clone(),
            move |event| {
                let deserted = match event {
                    ZmqEvent::Payload(payload) => tx.send(payload).is_err(),
                    ZmqEvent::Idle => tx.receiver_count() == 0,
                };
                if !deserted {
                    return ZmqFlow::Continue;
                }
                // Last subscriber is gone — shut the connection down. Checked
                // again under the lock: a new subscriber may have attached to
                // this very sender in the meantime, and stopping then would
                // hand it a stream that is dead on arrival.
                let mut sender = this.sender.lock().expect("zmq topic lock poisoned");
                if tx.receiver_count() > 0 {
                    return ZmqFlow::Continue;
                }
                *sender = None;
                tracing::debug!(
                    upstream = %this.upstream,
                    topic = %this.topic.id(),
                    "no subscribers left, closing the ZMQ topic connection"
                );
                ZmqFlow::Stop
            },
        ));
        rx
    }
}

/// Server-pushed subscriptions for a Bitcoin chain: the union of the ZMQ
/// topics its upstreams expose. Ports the legacy `BitcoinEgressSubscription`,
/// including merging the streams of every upstream that carries the requested
/// topic.
pub struct BitcoinEgress {
    sources: Vec<Arc<ZmqTopicStream>>,
}

impl BitcoinEgress {
    pub fn new(sources: Vec<Arc<ZmqTopicStream>>) -> Self {
        Self { sources }
    }
}

impl EgressSubscription for BitcoinEgress {
    /// `params` are ignored: no Bitcoin topic takes any (legacy did the same).
    fn subscribe(
        &self,
        method: &str,
        _params: Option<serde_json::Value>,
    ) -> Result<SubscriptionStream, EgressError> {
        let topic = BitcoinZmqTopic::find_by_id(method)
            .ok_or_else(|| EgressError::UnsupportedMethod(method.to_string()))?;
        let streams: Vec<_> = self
            .sources
            .iter()
            .filter(|s| s.topic() == topic)
            .map(|s| {
                BroadcastStream::new(s.subscribe())
                    // A lagged subscriber just skips ahead; delivery is
                    // best-effort by design.
                    .filter_map(|item| item.ok().map(|payload| payload.to_vec()))
            })
            .collect();
        if streams.is_empty() {
            return Err(EgressError::UnsupportedMethod(method.to_string()));
        }
        Ok(Box::pin(futures::stream::select_all(streams)))
    }

    fn available_topics(&self) -> Vec<String> {
        BitcoinZmqTopic::ALL
            .into_iter()
            .filter(|t| self.sources.iter().any(|s| s.topic() == *t))
            .map(|t| t.id().to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio_stream::StreamExt;
    use zeromq::{Socket, SocketSend, ZmqMessage};

    fn notification(topic: &str, payload: &[u8]) -> ZmqMessage {
        ZmqMessage::try_from(vec![
            Bytes::from(topic.as_bytes().to_vec()),
            Bytes::from(payload.to_vec()),
            Bytes::from(vec![0u8, 0, 0, 1]),
        ])
        .unwrap()
    }

    async fn bind_publisher() -> (zeromq::PubSocket, u16) {
        let mut publisher = zeromq::PubSocket::new();
        let endpoint = publisher.bind("tcp://127.0.0.1:0").await.unwrap();
        let port = match endpoint {
            zeromq::Endpoint::Tcp(_, port) => port,
            other => panic!("unexpected endpoint: {other:?}"),
        };
        (publisher, port)
    }

    #[test]
    fn topic_ids_round_trip() {
        for topic in BitcoinZmqTopic::ALL {
            assert_eq!(BitcoinZmqTopic::find_by_id(topic.id()), Some(topic));
        }
        assert_eq!(BitcoinZmqTopic::find_by_id("newHeads"), None);
    }

    #[test]
    fn unknown_topic_is_rejected() {
        let egress = BitcoinEgress::new(vec![]);
        assert!(matches!(
            egress.subscribe("hashtx", None),
            Err(EgressError::UnsupportedMethod(_))
        ));
        assert!(matches!(
            egress.subscribe("nonsense", None),
            Err(EgressError::UnsupportedMethod(_))
        ));
    }

    #[test]
    fn topics_reported_only_for_configured_sources() {
        let source = Arc::new(ZmqTopicStream::new(
            "127.0.0.1",
            1,
            BitcoinZmqTopic::Hashtx,
            "test-up".parse().unwrap(),
        ));
        let egress = BitcoinEgress::new(vec![source]);
        assert_eq!(egress.available_topics(), vec!["hashtx".to_string()]);
    }

    #[tokio::test]
    async fn delivers_raw_payload_to_egress_subscriber() {
        let (mut publisher, port) = bind_publisher().await;
        let source = Arc::new(ZmqTopicStream::new(
            "127.0.0.1",
            port,
            BitcoinZmqTopic::Hashtx,
            "test-up".parse().unwrap(),
        ));
        let egress = BitcoinEgress::new(vec![source]);

        let mut stream = egress.subscribe("hashtx", None).unwrap();
        let payload = vec![0x42u8; 32];
        let msg = notification("hashtx", &payload);

        // Publish until the subscription handshake completes and one gets
        // through.
        let received = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let _ = publisher.send(msg.clone()).await;
                tokio::select! {
                    item = stream.next() => return item,
                    _ = tokio::time::sleep(Duration::from_millis(50)) => continue,
                }
            }
        })
        .await
        .expect("no notification received");

        assert_eq!(received, Some(payload));
    }

    #[tokio::test]
    async fn connection_is_shared_and_stops_without_subscribers() {
        let (_publisher, port) = bind_publisher().await;
        let source = Arc::new(ZmqTopicStream::new(
            "127.0.0.1",
            port,
            BitcoinZmqTopic::Rawtx,
            "test-up".parse().unwrap(),
        ));

        let first = source.subscribe();
        let second = source.subscribe();
        // Both subscribers share the one connection started by the first.
        assert_eq!(
            source.sender.lock().unwrap().as_ref().unwrap().receiver_count(),
            2
        );

        drop(first);
        drop(second);
        // The task only notices on its next event — the idle tick at the
        // latest — so no immediate assertion on shutdown here; a new
        // subscriber must still get a working stream immediately.
        let _third = source.subscribe();
        assert!(source.sender.lock().unwrap().is_some());
    }
}
