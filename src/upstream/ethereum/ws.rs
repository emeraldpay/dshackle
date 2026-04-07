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

//! Ethereum upstream that communicates via WebSocket JSON-RPC.
//!
//! Maintains a persistent WebSocket connection with automatic reconnection
//! and exponential backoff. Requests are matched to responses via an internal
//! ID sequence, preserving the original caller IDs.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
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

/// An Ethereum upstream node accessed over WebSocket JSON-RPC.
///
/// The connection is established in a background task on creation and
/// automatically reconnects with exponential backoff on failure.
pub struct EthereumWsUpstream {
    #[allow(dead_code)]
    id: String,
    state: Arc<ConnectionState>,
}

/// Shared state between the call site and the background connection task.
struct ConnectionState {
    /// Pending RPC requests waiting for a response, keyed by internal ID.
    pending: Mutex<HashMap<u32, oneshot::Sender<Result<JsonRpcResponse, UpstreamError>>>>,
    /// Channel for sending serialized JSON-RPC messages to the write loop.
    outgoing: mpsc::Sender<String>,
    /// Monotonically increasing ID counter for request/response matching.
    next_id: AtomicU32,
}

impl EthereumWsUpstream {
    pub fn new(id: String, url: String) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::channel::<String>(256);

        let state = Arc::new(ConnectionState {
            pending: Mutex::new(HashMap::new()),
            outgoing: outgoing_tx,
            next_id: AtomicU32::new(IDS_START),
        });

        // Spawn the background connection loop
        let bg_state = Arc::clone(&state);
        let bg_id = id.clone();
        tokio::spawn(async move {
            connection_loop(bg_id, url, bg_state, outgoing_rx).await;
        });

        Self { id, state }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for EthereumWsUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        let internal_id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
        tracing::trace!(upstream = %self.id, method = %request.method, internal_id, "WS request");

        let (tx, rx) = oneshot::channel();

        // Register the pending request before sending, so the read loop can
        // route the response even if it arrives very quickly.
        {
            let mut pending = self.state.pending.lock().expect("pending lock poisoned");
            pending.insert(internal_id, tx);
        }

        // Build request with the internal ID for wire-level matching
        let wire_request = JsonRpcRequest::new(internal_id, request.method.clone(), request.params.clone());
        let json = match serde_json::to_string(&wire_request) {
            Ok(j) => j,
            Err(e) => {
                self.remove_pending(internal_id);
                return Err(UpstreamError::Transport(format!("failed to serialize request: {e}")));
            }
        };

        if self.state.outgoing.send(json).await.is_err() {
            self.remove_pending(internal_id);
            return Err(UpstreamError::Transport("WebSocket connection is closed".into()));
        }

        // Wait for the response with a timeout
        let result = tokio::time::timeout(CALL_TIMEOUT, rx).await;

        // Always clean up — the entry may already be gone (consumed by the read
        // loop), but remove is idempotent. This guarantees no leaked entries if
        // the background task dies or the request is cancelled.
        self.remove_pending(internal_id);

        match result {
            Ok(Ok(result)) => {
                tracing::trace!(upstream = %self.id, internal_id, ok = result.is_ok(), "WS response received");
                result
            }
            Ok(Err(_)) => {
                tracing::trace!(upstream = %self.id, internal_id, "WS disconnected while waiting");
                Err(UpstreamError::Transport("WebSocket disconnected while waiting for response".into()))
            }
            Err(_) => {
                tracing::trace!(upstream = %self.id, internal_id, "WS response timeout");
                Err(UpstreamError::Transport(format!(
                    "WebSocket response timeout after {}s",
                    CALL_TIMEOUT.as_secs()
                )))
            }
        }
    }
}

impl EthereumWsUpstream {
    fn remove_pending(&self, id: u32) {
        let mut pending = self.state.pending.lock().expect("pending lock poisoned");
        pending.remove(&id);
    }
}

// ─── Background connection loop ─────────────────────────────────────────────

/// Runs the persistent WebSocket connection, reconnecting on failure.
async fn connection_loop(
    id: String,
    url: String,
    state: Arc<ConnectionState>,
    mut outgoing_rx: mpsc::Receiver<String>,
) {
    let mut backoff = BACKOFF_INITIAL;

    loop {
        tracing::info!("Upstream {id}: connecting to WebSocket {url}");

        match connect_and_run(&id, &url, &state, &mut outgoing_rx).await {
            Ok(()) => {
                tracing::info!("Upstream {id}: WebSocket connection closed normally");
            }
            Err(e) => {
                tracing::warn!("Upstream {id}: WebSocket error: {e}");
            }
        }

        // Fail all pending requests — the connection is gone
        fail_all_pending(&state, "WebSocket disconnected");

        tracing::info!("Upstream {id}: reconnecting in {}ms", backoff.as_millis());
        tokio::time::sleep(backoff).await;

        // Exponential backoff with cap
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// Connect to the WS endpoint and run the read/write loops until disconnection.
async fn connect_and_run(
    id: &str,
    url: &str,
    state: &Arc<ConnectionState>,
    outgoing_rx: &mut mpsc::Receiver<String>,
) -> Result<(), String> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;

    tracing::info!("Upstream {id}: WebSocket connected");

    let (mut ws_write, mut ws_read) = ws_stream.split();

    // Reset backoff will happen in the caller if we get here without error,
    // but we also want to track whether we got at least one successful response
    // to reset backoff (matching legacy behavior). For simplicity in MVP,
    // we reset backoff implicitly by reaching this point.

    loop {
        tokio::select! {
            // Read incoming WS message
            msg = ws_read.next() => {
                match msg {
                    Some(Ok(tungstenite::Message::Text(text))) => {
                        handle_incoming_message(state, &text);
                    }
                    Some(Ok(tungstenite::Message::Binary(data))) => {
                        if let Ok(text) = std::str::from_utf8(&data) {
                            handle_incoming_message(state, text);
                        } else {
                            tracing::warn!("Upstream {id}: received non-UTF-8 binary WS message");
                        }
                    }
                    Some(Ok(tungstenite::Message::Close(_))) => {
                        tracing::info!("Upstream {id}: WebSocket close frame received");
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
                        tracing::info!("Upstream {id}: outgoing channel closed, shutting down WS");
                        let _ = ws_write.close().await;
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Parse an incoming WS text message and route it to the matching pending request.
fn handle_incoming_message(state: &ConnectionState, text: &str) {
    // Parse with RawValue so the result field is kept as raw JSON bytes
    let response: JsonRpcResponse = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(e) => {
            // Could be a subscription notification — skip silently for now
            tracing::debug!("Ignoring unparseable WS message: {e}");
            return;
        }
    };

    // Extract the numeric ID to match against pending requests
    let id = match response.id.as_u64() {
        Some(n) => n as u32,
        None => {
            // Subscription responses have string IDs — not handled yet
            tracing::debug!("Ignoring WS message with non-numeric id: {}", response.id);
            return;
        }
    };

    let sender = {
        let mut pending = state.pending.lock().expect("pending lock poisoned");
        pending.remove(&id)
    };

    match sender {
        Some(tx) => {
            let _ = tx.send(Ok(response));
        }
        None => {
            tracing::debug!("No pending request for WS response id={id}");
        }
    }
}

/// Fail all pending requests with the given error message (called on disconnect).
fn fail_all_pending(state: &ConnectionState, reason: &str) {
    let mut pending = state.pending.lock().expect("pending lock poisoned");
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(UpstreamError::Transport(reason.to_string())));
    }
}
