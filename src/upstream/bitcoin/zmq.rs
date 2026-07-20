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

//! Bitcoin head updates pushed over ZeroMQ.
//!
//! A bitcoind started with `-zmqpubhashblock` announces every connected block
//! on the `hashblock` topic, letting a new block reach the head in
//! milliseconds instead of waiting out the 15-second poll cycle. The RPC
//! poller stays running as the safety net: ZMQ only announces *new* blocks, so
//! the poller provides the initial head and covers any gap in the ZMQ
//! delivery. Both sources feed the same [`CurrentHead`], which dedups repeats
//! by hash — the same merge the legacy `MergedPowHead` performed over its
//! `BitcoinRpcHead` + `BitcoinZMQHead` pair.
//!
//! # Liveness
//!
//! The pure-Rust `zeromq` SUB socket gives no usable failure signal: a dropped
//! peer is removed silently (`recv()` then pends forever rather than erroring,
//! and only REP sockets emit monitor events), while `connect()` retries a
//! refused connection internally without ever returning. So liveness is
//! enforced from outside: the subscription is torn down and rebuilt whenever
//! it stays quiet past [`IDLE_TIMEOUT`], and the initial connect is bounded by
//! [`CONNECT_TIMEOUT`] purely so an unreachable endpoint gets logged. A
//! rebuild costs one TCP handshake and the subscription carries no state, so
//! the only downside is a milliseconds-wide window where a notification can be
//! missed — which the RPC poller covers anyway.

use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::bitcoin::head::apply_block_response;
use crate::upstream::head::CurrentHead;
use crate::upstream::traits::RpcUpstream;
use std::sync::Arc;
use std::time::Duration;
use zeromq::{Socket, SocketRecv};

/// Topic bitcoind publishes the 32-byte hash of every connected block to.
const HASHBLOCK_TOPIC: &str = "hashblock";

/// Bound on the initial connect, which the `zeromq` crate otherwise retries
/// internally forever: without it a wrong port or a down node would produce no
/// log signal at all.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Rebuild the subscription when nothing arrived for this long. Bitcoin
/// routinely goes longer than this between blocks — the point is not to detect
/// staleness but to bound the recovery time after a silently dropped
/// connection, since the socket gives no signal for it.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Delay between reconnection attempts after a failed connect.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// `getblock` retry schedule for a just-announced hash: the notification can
/// arrive a moment before the node is ready to serve the block. Same schedule
/// as the legacy `BitcoinZMQHead` (5 retries backing off from 100ms).
const FETCH_RETRIES: u32 = 5;
const FETCH_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Spawns a background task that subscribes to `hashblock` notifications at
/// `host:port`, resolves each announced hash to a full block through the
/// upstream's RPC, and pushes it into the shared head tracker.
pub fn start_zmq_head(
    host: String,
    port: u16,
    upstream: Arc<dyn RpcUpstream>,
    head: Arc<CurrentHead>,
) {
    tokio::spawn(async move {
        let endpoint = format!("tcp://{host}:{port}");
        tracing::info!(upstream = %upstream.id(), "Connecting to ZMQ at {}:{}", host, port);
        // The endpoint being down is a routine state the poller fully covers,
        // so it is announced once at warn and the once-a-second retries stay
        // at debug until the connection recovers.
        let mut reported_down = false;
        loop {
            let mut socket = zeromq::SubSocket::new();
            match tokio::time::timeout(CONNECT_TIMEOUT, subscribe(&mut socket, &endpoint)).await {
                Ok(Ok(())) => {
                    if reported_down {
                        reported_down = false;
                        tracing::info!(
                            upstream = %upstream.id(),
                            "ZMQ connection to {}:{} established",
                            host,
                            port
                        );
                    }
                    listen(&mut socket, &upstream, &head).await;
                }
                Ok(Err(e)) => {
                    if !reported_down {
                        reported_down = true;
                        tracing::warn!(
                            upstream = %upstream.id(),
                            error = %e,
                            "Failed to connect to ZMQ at {}:{}",
                            host,
                            port
                        );
                    } else {
                        tracing::debug!(upstream = %upstream.id(), error = %e, "ZMQ still failing to connect");
                    }
                }
                Err(_) => {
                    if !reported_down {
                        reported_down = true;
                        tracing::warn!(
                            upstream = %upstream.id(),
                            "ZMQ at {}:{} is not reachable",
                            host,
                            port
                        );
                    } else {
                        tracing::debug!(upstream = %upstream.id(), "ZMQ still not reachable");
                    }
                }
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    });
}

async fn subscribe(socket: &mut zeromq::SubSocket, endpoint: &str) -> zeromq::ZmqResult<()> {
    socket.connect(endpoint).await?;
    socket.subscribe(HASHBLOCK_TOPIC).await
}

/// Receives notifications until the subscription goes quiet for
/// [`IDLE_TIMEOUT`] (or errors); the caller then rebuilds it.
async fn listen(
    socket: &mut zeromq::SubSocket,
    upstream: &Arc<dyn RpcUpstream>,
    head: &Arc<CurrentHead>,
) {
    loop {
        match tokio::time::timeout(IDLE_TIMEOUT, socket.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(hash) = hashblock_hash(&msg) {
                    // Resolved concurrently: a hash the node can't serve yet
                    // must not hold up the next notification behind its retry
                    // schedule (legacy fetched with `flatMap`, also
                    // concurrently).
                    tokio::spawn(fetch_block(Arc::clone(upstream), Arc::clone(head), hash));
                }
            }
            Ok(Err(e)) => {
                tracing::debug!(upstream = %upstream.id(), error = %e, "ZMQ receive failed, reconnecting");
                return;
            }
            Err(_) => {
                tracing::debug!(upstream = %upstream.id(), "no ZMQ traffic, rebuilding the subscription");
                return;
            }
        }
    }
}

/// The announced block hash, if the message is a well-formed `hashblock`
/// notification — a `[topic, 32-byte hash, uint32 sequence]` triple.
///
/// bitcoind reverses its internal byte order before publishing, so the payload
/// hex-encodes directly into the display-order string `getblock` expects.
fn hashblock_hash(msg: &zeromq::ZmqMessage) -> Option<String> {
    let topic = msg.get(0)?;
    // ZMQ subscriptions match by prefix; accept only the exact topic.
    if topic.as_ref() != HASHBLOCK_TOPIC.as_bytes() {
        return None;
    }
    let payload = msg.get(1)?;
    if payload.len() != 32 {
        return None;
    }
    Some(hex::encode(payload))
}

/// Resolves an announced hash to the full block and feeds the head.
async fn fetch_block(upstream: Arc<dyn RpcUpstream>, head: Arc<CurrentHead>, hash: String) {
    let request = JsonRpcRequest::new(0, "getblock".into(), serde_json::json!([hash, 1]));
    let mut backoff = FETCH_INITIAL_BACKOFF;
    for attempt in 0..=FETCH_RETRIES {
        if attempt > 0 {
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
        match upstream.call(&request).await {
            Ok(resp) => {
                if let Some(raw) = resp.result.as_ref() {
                    apply_block_response(upstream.id(), &head, raw.get());
                    return;
                }
                // No result yet: the block was announced before the node can
                // serve it — retry.
            }
            Err(e) => tracing::debug!(
                upstream = %upstream.id(),
                error = %e,
                "getblock after ZMQ notification failed"
            ),
        }
    }
    tracing::warn!(upstream = %upstream.id(), "Block {} is not available on upstream", hash);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::UpstreamRole;
    use crate::jsonrpc::JsonRpcResponse;
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::id::UpstreamId;
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::UpstreamError;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU32, Ordering};
    use zeromq::{SocketSend, ZmqMessage};

    const BLOCK_HASH: &str = "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6";

    /// Serves `getblock` after failing the first `fail_first` calls with an
    /// empty result, imitating a node that announced a block it can't serve yet.
    struct BlockUpstream {
        calls: AtomicU32,
        fail_first: u32,
    }

    impl BlockUpstream {
        fn new(fail_first: u32) -> Self {
            Self {
                calls: AtomicU32::new(0),
                fail_first,
            }
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for BlockUpstream {
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            assert_eq!(request.method.as_str(), "getblock");
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            let raw = if call < self.fail_first {
                r#"{"jsonrpc":"2.0","id":1,"result":null}"#.to_string()
            } else {
                format!(
                    r#"{{"jsonrpc":"2.0","id":1,"result":{{"hash":"{BLOCK_HASH}","height":800000,"time":1690000000}}}}"#
                )
            };
            Ok(serde_json::from_str(&raw).unwrap())
        }
        fn id(&self) -> &UpstreamId {
            static ID: std::sync::LazyLock<UpstreamId> =
                std::sync::LazyLock::new(|| "mock-btc".parse().unwrap());
            &ID
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
            static STATE: std::sync::LazyLock<Arc<UpstreamState>> =
                std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));
            &STATE
        }
        fn role(&self) -> UpstreamRole {
            UpstreamRole::Primary
        }
    }

    fn hashblock_message(topic: &str, payload: Vec<u8>) -> ZmqMessage {
        ZmqMessage::try_from(vec![
            Bytes::from(topic.as_bytes().to_vec()),
            Bytes::from(payload),
            Bytes::from(vec![0u8, 0, 0, 1]),
        ])
        .unwrap()
    }

    #[test]
    fn extracts_hash_from_notification() {
        let msg = hashblock_message("hashblock", vec![0xAB; 32]);
        assert_eq!(hashblock_hash(&msg), Some("ab".repeat(32)));
    }

    #[test]
    fn rejects_other_topics_and_malformed_payload() {
        assert_eq!(
            hashblock_hash(&hashblock_message("hashtx", vec![0xAB; 32])),
            None
        );
        // Prefix-matching a longer topic must not pass the exact check.
        assert_eq!(
            hashblock_hash(&hashblock_message("hashblocks", vec![0xAB; 32])),
            None
        );
        assert_eq!(
            hashblock_hash(&hashblock_message("hashblock", vec![0xAB; 16])),
            None
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fetch_retries_until_block_is_served() {
        let upstream = Arc::new(BlockUpstream::new(2));
        let head = Arc::new(CurrentHead::new());

        fetch_block(
            Arc::clone(&upstream) as Arc<dyn RpcUpstream>,
            Arc::clone(&head),
            BLOCK_HASH.to_string(),
        )
        .await;

        assert_eq!(head.current_height(), Some(800_000));
        assert_eq!(upstream.calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn fetch_gives_up_after_retries() {
        // Never serves the block: initial attempt + 5 retries, then give up.
        let upstream = Arc::new(BlockUpstream::new(u32::MAX));
        let head = Arc::new(CurrentHead::new());

        fetch_block(
            Arc::clone(&upstream) as Arc<dyn RpcUpstream>,
            Arc::clone(&head),
            BLOCK_HASH.to_string(),
        )
        .await;

        assert_eq!(head.current_height(), None);
        assert_eq!(upstream.calls.load(Ordering::Relaxed), 6);
    }

    #[tokio::test]
    async fn receives_block_announced_over_zmq() {
        // A local PUB socket stands in for bitcoind's -zmqpubhashblock.
        let mut publisher = zeromq::PubSocket::new();
        let endpoint = publisher.bind("tcp://127.0.0.1:0").await.unwrap();
        let port = match endpoint {
            zeromq::Endpoint::Tcp(_, port) => port,
            other => panic!("unexpected endpoint: {other:?}"),
        };

        let head = Arc::new(CurrentHead::new());
        start_zmq_head(
            "127.0.0.1".to_string(),
            port,
            Arc::new(BlockUpstream::new(0)),
            Arc::clone(&head),
        );

        // The subscription handshake isn't observable from the publisher, so
        // keep announcing until the block makes it through.
        let hash = hex::decode(BLOCK_HASH).unwrap();
        let msg = hashblock_message(HASHBLOCK_TOPIC, hash);
        let received = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let _ = publisher.send(msg.clone()).await;
                if head.current_height().is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;

        assert!(received.is_ok(), "no block received over ZMQ");
        assert_eq!(head.current_height(), Some(800_000));
    }
}
