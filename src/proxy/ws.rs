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

//! JSON-RPC over WebSocket for the proxy.
//!
//! Serves the same `/<route-id>` endpoints as the HTTP proxy, over a persistent
//! WebSocket connection. Each text frame is one JSON-RPC request: plain calls
//! go through the shared call path, while `eth_subscribe` / `eth_unsubscribe`
//! drive server-pushed [`EgressSubscription`] streams. Ports the legacy
//! `WebsocketHandler`.

use super::ProxyRoute;
use super::handler;
use super::protocol::{self, BodyKind, ProxyRequest};
use crate::upstream::egress::EgressError;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use warp::ws::{Message, WebSocket};

const METHOD_SUBSCRIBE: &str = "eth_subscribe";
const METHOD_UNSUBSCRIBE: &str = "eth_unsubscribe";

/// Code returned for a subscription request that can't be served.
const CODE_METHOD_NOT_FOUND: i64 = -32601;

/// Bound on per-connection buffered outbound messages. A slow or stalled
/// reader fills this and then back-pressures its producers — request tasks and
/// subscription pumps block on send — instead of letting the queue grow without
/// limit (the legacy reactive pipeline applied backpressure the same way). Big
/// enough to absorb normal bursts (batched responses, a head fan-out across a
/// few subscriptions) without stalling a healthy client.
const OUTBOUND_CHANNEL_CAPACITY: usize = 256;

/// Connection-independent subscription id counter. The legacy proxy keeps one
/// `AtomicLong` per handler instance (effectively global); a process-wide
/// counter keeps ids unique across connections too. Hex-encoded, matching
/// `Long.toString(16)`.
static NEXT_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);

fn next_subscription_id() -> String {
    format!("{:x}", NEXT_SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed))
}

/// Serve one accepted WebSocket connection until the client disconnects.
///
/// Plain calls are processed concurrently (one task per frame, matching the
/// legacy `flatMap`) and answered as they complete. A single writer task owns
/// the sink, so request responses and subscription pushes never race on it, and
/// the bounded channel back-pressures producers when the client can't keep up.
/// The per-connection subscription registry is touched only by this read loop,
/// so it needs no locking.
pub async fn serve(socket: WebSocket, route: Arc<ProxyRoute>) {
    let (mut sink, mut stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(OUTBOUND_CHANNEL_CAPACITY);

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Active subscriptions on this connection: id -> handle to its pump task.
    let mut subscriptions: HashMap<String, AbortHandle> = HashMap::new();

    while let Some(frame) = stream.next().await {
        let msg = match frame {
            Ok(msg) => msg,
            // Connection error or reset: stop reading.
            Err(_) => break,
        };
        if msg.is_close() {
            break;
        }
        // Only data frames carry requests; warp answers ping/pong itself.
        if !msg.is_text() && !msg.is_binary() {
            continue;
        }

        // Malformed input and batches are silently ignored, as the legacy proxy
        // does ("that's what other Ethereum servers do").
        let Some(req) = parse_single(msg.as_bytes()) else {
            continue;
        };

        match req.method.as_str() {
            METHOD_SUBSCRIBE => handle_subscribe(&route, &req, &out_tx, &mut subscriptions).await,
            METHOD_UNSUBSCRIBE => handle_unsubscribe(&req, &out_tx, &mut subscriptions).await,
            _ => {
                // Plain call: run it off the read loop so a slow upstream never
                // blocks later frames on this connection.
                let multistream = Arc::clone(&route.multistream);
                let out_tx = out_tx.clone();
                tokio::spawn(async move {
                    let response = handler::run(&multistream, &req).await;
                    let _ = out_tx.send(Message::text(response)).await;
                });
            }
        }
    }

    // Stop all subscription pumps, then let the writer drain once the in-flight
    // call tasks (which hold their own sender clones) finish.
    for (_, handle) in subscriptions.drain() {
        handle.abort();
    }
    drop(out_tx);
    let _ = writer.await;
}

/// Parse one frame as a single JSON-RPC request, or `None` for anything to
/// ignore (a batch — unsupported over WS — or malformed input).
fn parse_single(bytes: &[u8]) -> Option<ProxyRequest> {
    if !matches!(protocol::detect_kind(bytes), Ok(BodyKind::Single)) {
        return None;
    }
    let value = protocol::parse_body(bytes).ok()?;
    protocol::validate_item(&value).ok()
}

/// Start a subscription: respond with its id, then pump its events back wrapped
/// in the `eth_subscription` envelope.
async fn handle_subscribe(
    route: &ProxyRoute,
    req: &ProxyRequest,
    out_tx: &mpsc::Sender<Message>,
    subscriptions: &mut HashMap<String, AbortHandle>,
) {
    // A malformed subscribe (no topic, or too many params) is ignored, matching
    // the legacy `splitMethodParams` returning null.
    let Some((topic, params)) = subscribe_params(&req.params) else {
        return;
    };

    let Some(egress) = &route.egress else {
        let _ = out_tx
            .send(Message::text(protocol::build_error(
                &req.id,
                CODE_METHOD_NOT_FOUND,
                "Subscriptions are not supported for this blockchain".to_string(),
                None,
            )))
            .await;
        return;
    };

    match egress.subscribe(&topic, params) {
        Ok(mut stream) => {
            let sub_id = next_subscription_id();
            // The id response must precede any pushed event; the channel is FIFO
            // and this enqueues before the pump task is spawned.
            let _ = out_tx
                .send(Message::text(protocol::build_success_value(
                    &req.id,
                    serde_json::Value::String(sub_id.clone()),
                )))
                .await;

            let out = out_tx.clone();
            let id = sub_id.clone();
            let handle = tokio::spawn(async move {
                while let Some(event) = stream.next().await {
                    // Bad event JSON or a closed channel stops the pump.
                    let Some(msg) = subscription_message(&id, &event) else {
                        break;
                    };
                    if out.send(Message::text(msg)).await.is_err() {
                        break;
                    }
                }
            });
            subscriptions.insert(sub_id, handle.abort_handle());
        }
        Err(EgressError::UnsupportedMethod(method)) => {
            let _ = out_tx
                .send(Message::text(protocol::build_error(
                    &req.id,
                    CODE_METHOD_NOT_FOUND,
                    format!("Subscription type {method} is not supported"),
                    None,
                )))
                .await;
        }
    }
}

/// Cancel a subscription by id and report whether one was active.
async fn handle_unsubscribe(
    req: &ProxyRequest,
    out_tx: &mpsc::Sender<Message>,
    subscriptions: &mut HashMap<String, AbortHandle>,
) {
    let removed = req
        .params
        .as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .and_then(|id| subscriptions.remove(id));
    if let Some(handle) = &removed {
        handle.abort();
    }
    let _ = out_tx
        .send(Message::text(protocol::build_success_value(
            &req.id,
            serde_json::Value::Bool(removed.is_some()),
        )))
        .await;
}

/// Extract `(topic, params)` from a subscribe request's params array. Ports the
/// legacy `splitMethodParams`: one or two elements only.
fn subscribe_params(params: &serde_json::Value) -> Option<(String, Option<serde_json::Value>)> {
    let arr = params.as_array()?;
    match arr.len() {
        1 => Some((arr[0].as_str()?.to_string(), None)),
        2 => Some((arr[0].as_str()?.to_string(), Some(arr[1].clone()))),
        _ => None,
    }
}

/// Wrap a subscription event in the `eth_subscription` envelope. Returns `None`
/// if the event isn't valid JSON (it always is, coming from the egress).
fn subscription_message(sub_id: &str, event: &[u8]) -> Option<String> {
    let result: serde_json::Value = serde_json::from_slice(event).ok()?;
    let envelope = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_subscription",
        "params": { "subscription": sub_id, "result": result },
    });
    serde_json::to_string(&envelope).ok()
}
