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

//! A single persistent WebSocket connection with automatic reconnection
//! and exponential backoff. Requests are matched to responses via an internal
//! ID sequence, preserving the original caller IDs.
//!
//! Also supports `eth_subscribe`-style subscriptions, which return a channel
//! of raw JSON notification payloads tied to this specific connection.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::traits::UpstreamError;
use futures_util::{SinkExt, StreamExt};
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite;

/// Starting ID for the internal request sequence, matching the legacy implementation.
const IDS_START: u32 = 100;

/// How long to wait for a response before giving up.
const CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Initial reconnection delay.
const BACKOFF_INITIAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Maximum reconnection delay.
const BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(300);

/// A single WebSocket connection that manages its own background reconnection
/// loop. Each connection has independent pending-request and subscription maps,
/// its own outgoing message channel, and tracks connected/disconnected state.
pub(super) struct WsConnection {
    /// Human-readable label for logging (e.g. "Upstream my-node" or "Upstream my-node/2").
    label: String,
    inner: Arc<WsConnectionInner>,
}

/// Shared state for a single WS connection, accessed by both the call site
/// and the background connection task.
struct WsConnectionInner {
    /// Pending RPC requests waiting for a response, keyed by internal ID.
    pending: Mutex<HashMap<u32, oneshot::Sender<Result<JsonRpcResponse, UpstreamError>>>>,
    /// Active subscriptions: node-assigned subscription ID -> sender for notification payloads.
    subscriptions: Mutex<HashMap<String, mpsc::UnboundedSender<Box<RawValue>>>>,
    /// Channel for sending serialized JSON-RPC messages to the write loop.
    outgoing: mpsc::Sender<String>,
    /// Monotonically increasing ID counter for request/response matching.
    next_id: AtomicU32,
    /// Whether this connection is currently established and ready.
    connected: AtomicBool,
}

impl WsConnection {
    /// Create a new WS connection and start its background reconnection loop.
    pub(super) fn new(label: String, url: String) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::channel::<String>(256);

        let inner = Arc::new(WsConnectionInner {
            pending: Mutex::new(HashMap::new()),
            subscriptions: Mutex::new(HashMap::new()),
            outgoing: outgoing_tx,
            next_id: AtomicU32::new(IDS_START),
            connected: AtomicBool::new(false),
        });

        let bg_inner = Arc::clone(&inner);
        let bg_label = label.clone();
        tokio::spawn(async move {
            connection_loop(bg_label, url, bg_inner, outgoing_rx).await;
        });

        Self { label, inner }
    }

    /// Whether this connection is currently established.
    pub(super) fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::Relaxed)
    }

    /// Execute a JSON-RPC request over this connection.
    pub(super) async fn call(
        &self,
        request: &JsonRpcRequest,
    ) -> Result<JsonRpcResponse, UpstreamError> {
        let internal_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        tracing::trace!(connection = %self.label, method = %request.method, internal_id, "WS request");

        let (tx, rx) = oneshot::channel();

        // Register before sending so the read loop can route even very fast responses.
        {
            let mut pending = self.inner.pending.lock().expect("pending lock poisoned");
            pending.insert(internal_id, tx);
        }

        // Build request with the internal ID for wire-level matching
        let wire_request =
            JsonRpcRequest::new(internal_id, request.method.clone(), request.params.clone());
        let json = match serde_json::to_string(&wire_request) {
            Ok(j) => j,
            Err(e) => {
                self.remove_pending(internal_id);
                return Err(UpstreamError::Transport(format!(
                    "failed to serialize request: {e}"
                )));
            }
        };

        if self.inner.outgoing.send(json).await.is_err() {
            self.remove_pending(internal_id);
            return Err(UpstreamError::Transport(
                "WebSocket connection is closed".into(),
            ));
        }

        let result = tokio::time::timeout(CALL_TIMEOUT, rx).await;

        // Always clean up — the entry may already be gone (consumed by the read
        // loop), but remove is idempotent.
        self.remove_pending(internal_id);

        match result {
            Ok(Ok(result)) => {
                tracing::trace!(connection = %self.label, internal_id, ok = result.is_ok(), "WS response received");
                result
            }
            Ok(Err(_)) => {
                tracing::trace!(connection = %self.label, internal_id, "WS disconnected while waiting");
                Err(UpstreamError::Transport(
                    "WebSocket disconnected while waiting for response".into(),
                ))
            }
            Err(_) => {
                tracing::trace!(connection = %self.label, internal_id, "WS response timeout");
                Err(UpstreamError::Transport(format!(
                    "WebSocket response timeout after {}s",
                    CALL_TIMEOUT.as_secs()
                )))
            }
        }
    }

    /// Subscribe to an `eth_subscribe` topic on this specific connection.
    ///
    /// The subscription is tied to this connection — when it drops, the
    /// returned receiver closes.
    pub(super) async fn subscribe(
        &self,
        topic: &str,
    ) -> Result<mpsc::UnboundedReceiver<Box<RawValue>>, UpstreamError> {
        let request = JsonRpcRequest::new(0, "eth_subscribe".into(), serde_json::json!([topic]));
        let response = self.call(&request).await?;

        let sub_id = response.result.ok_or_else(|| {
            UpstreamError::InvalidResponse("eth_subscribe returned no result".into())
        })?;
        let sub_id = sub_id.get().trim().trim_matches('"').to_string();

        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut subs = self
                .inner
                .subscriptions
                .lock()
                .expect("subscriptions lock poisoned");
            subs.insert(sub_id.clone(), tx);
        }

        tracing::debug!(connection = %self.label, sub_id = %sub_id, topic, "subscription active");
        Ok(rx)
    }

    fn remove_pending(&self, id: u32) {
        let mut pending = self.inner.pending.lock().expect("pending lock poisoned");
        pending.remove(&id);
    }
}

// ─── Background connection loop ─────────────────────────────────────────────

/// Runs the persistent WebSocket connection, reconnecting on failure.
async fn connection_loop(
    label: String,
    url: String,
    state: Arc<WsConnectionInner>,
    mut outgoing_rx: mpsc::Receiver<String>,
) {
    let mut backoff = BACKOFF_INITIAL;

    loop {
        tracing::info!("{label}: connecting to WebSocket {url}");

        match connect_and_run(&label, &url, &state, &mut outgoing_rx).await {
            Ok(()) => {
                tracing::info!("{label}: WebSocket connection closed normally");
            }
            Err(e) => {
                tracing::warn!("{label}: WebSocket error: {e}");
            }
        }

        // Reset backoff when the connection was successfully established —
        // the disconnect is transient, not a persistent connect failure.
        let was_connected = state.connected.swap(false, Ordering::Relaxed);
        if was_connected {
            backoff = BACKOFF_INITIAL;
        }

        // Fail all pending requests and close all subscriptions
        cleanup_on_disconnect(&state, "WebSocket disconnected");

        tracing::info!("{label}: reconnecting in {}ms", backoff.as_millis());
        tokio::time::sleep(backoff).await;

        // Exponential backoff with cap
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// Connect to the WS endpoint and run the read/write loops until disconnection.
async fn connect_and_run(
    label: &str,
    url: &str,
    state: &Arc<WsConnectionInner>,
    outgoing_rx: &mut mpsc::Receiver<String>,
) -> Result<(), String> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;

    state.connected.store(true, Ordering::Relaxed);
    tracing::info!("{label}: WebSocket connected");

    let (mut ws_write, mut ws_read) = ws_stream.split();

    loop {
        tokio::select! {
            // Read incoming WS message
            msg = ws_read.next() => {
                match msg {
                    Some(Ok(tungstenite::Message::Text(text))) => {
                        handle_incoming_message(label, state, &text);
                    }
                    Some(Ok(tungstenite::Message::Binary(data))) => {
                        if let Ok(text) = std::str::from_utf8(&data) {
                            handle_incoming_message(label, state, text);
                        } else {
                            tracing::warn!("{label}: received non-UTF-8 binary WS message");
                        }
                    }
                    Some(Ok(tungstenite::Message::Close(_))) => {
                        tracing::info!("{label}: WebSocket close frame received");
                        return Ok(());
                    }
                    Some(Ok(_)) => {
                        // Ping/Pong frames handled automatically by tungstenite
                    }
                    Some(Err(e)) => {
                        return Err(format!("read error: {e}"));
                    }
                    None => {
                        // Stream ended
                        return Ok(());
                    }
                }
            }

            // Write outgoing message from the call site
            outgoing = outgoing_rx.recv() => {
                match outgoing {
                    Some(json) => {
                        if let Err(e) = ws_write.send(tungstenite::Message::Text(json.into())).await {
                            return Err(format!("write error: {e}"));
                        }
                    }
                    None => {
                        // All senders dropped — upstream is being shut down
                        tracing::info!("{label}: outgoing channel closed, shutting down WS");
                        let _ = ws_write.close().await;
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Route an incoming WS message to either a pending RPC caller or a subscription.
fn handle_incoming_message(label: &str, state: &WsConnectionInner, text: &str) {
    // Try subscription notification first (has "method" field, no numeric "id")
    if let Ok(notif) = serde_json::from_str::<SubscriptionNotification>(text) {
        if notif.method == "eth_subscription" {
            let mut subs = state
                .subscriptions
                .lock()
                .expect("subscriptions lock poisoned");
            if let Some(tx) = subs.get(&notif.params.subscription) {
                if tx.send(notif.params.result).is_err() {
                    tracing::debug!(
                        "{label}: subscription receiver dropped for {}, removing",
                        notif.params.subscription,
                    );
                    subs.remove(&notif.params.subscription);
                }
            }
            return;
        }
    }

    // Standard JSON-RPC response — route to the matching pending request
    let response: JsonRpcResponse = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!("{label}: ignoring unparseable WS message: {e}");
            return;
        }
    };

    let msg_id = match response.id.as_u64() {
        Some(n) => n as u32,
        None => {
            tracing::debug!(
                "{label}: ignoring WS message with non-numeric id: {}",
                response.id
            );
            return;
        }
    };

    let sender = {
        let mut pending = state.pending.lock().expect("pending lock poisoned");
        pending.remove(&msg_id)
    };

    match sender {
        Some(tx) => {
            let _ = tx.send(Ok(response));
        }
        None => {
            tracing::debug!("{label}: no pending request for WS response id={msg_id}");
        }
    }
}

/// Fail all pending requests and close all subscription channels on disconnect.
fn cleanup_on_disconnect(state: &WsConnectionInner, reason: &str) {
    {
        let mut pending = state.pending.lock().expect("pending lock poisoned");
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(UpstreamError::Transport(reason.to_string())));
        }
    }
    {
        // Dropping senders closes the receivers, signalling disconnect to subscribers
        let mut subs = state
            .subscriptions
            .lock()
            .expect("subscriptions lock poisoned");
        subs.clear();
    }
}

// ─── Subscription notification wire types ───────────────────────────────────

/// An `eth_subscription` notification from the node.
#[derive(serde::Deserialize)]
struct SubscriptionNotification {
    method: String,
    params: SubscriptionParams,
}

#[derive(serde::Deserialize)]
struct SubscriptionParams {
    subscription: String,
    result: Box<RawValue>,
}
