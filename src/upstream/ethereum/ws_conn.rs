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
//! Also supports `eth_subscribe`-style subscriptions, which return a
//! [`WsSubscription`] of raw JSON notification payloads tied to this specific
//! connection.

use crate::config::tls::BasicAuth;
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::pause::{PauseHandle, PauseReason};
use crate::upstream::quorum::is_unavailable_status;
use crate::upstream::traits::UpstreamError;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

/// Where and how to open a WS connection to the node.
#[derive(Clone)]
pub struct WsTarget {
    pub url: String,
    pub origin: Option<String>,
    pub basic_auth: Option<BasicAuth>,
    /// How long to wait for a call's response frame (the upstream's
    /// `options.timeout`).
    pub call_timeout: std::time::Duration,
    /// Incoming frame payload cap; `None` keeps the tungstenite default
    /// (16 MiB) — deliberately roomier than legacy's 5 MiB.
    pub frame_size: Option<usize>,
    /// Incoming message cap after frame aggregation; `None` keeps the
    /// tungstenite default (64 MiB) — deliberately roomier than legacy's
    /// 15 MiB.
    pub msg_size: Option<usize>,
}

impl WsTarget {
    /// The WS handshake request, with the extra headers the legacy client
    /// always sent: `Origin` (defaulting to http://localhost) and the
    /// optional basic auth.
    fn client_request(&self) -> Result<tungstenite::handshake::client::Request, String> {
        let mut request = self
            .url
            .as_str()
            .into_client_request()
            .map_err(|e| format!("invalid WS URL: {e}"))?;
        let headers = request.headers_mut();
        let origin = self.origin.as_deref().unwrap_or("http://localhost");
        headers.insert(
            "Origin",
            origin
                .parse()
                .map_err(|e| format!("invalid WS origin: {e}"))?,
        );
        headers.insert(
            tungstenite::http::header::USER_AGENT,
            tungstenite::http::HeaderValue::from_static(crate::upstream::USER_AGENT),
        );
        if let Some(auth) = &self.basic_auth {
            let token = base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", auth.username, auth.password));
            headers.insert(
                "Authorization",
                format!("Basic {token}")
                    .parse()
                    .map_err(|e| format!("invalid WS basic auth: {e}"))?,
            );
        }
        Ok(request)
    }
}

/// Starting ID for the internal request sequence, matching the legacy implementation.
const IDS_START: u32 = 100;

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
    /// How long to wait for a call's response frame before giving up.
    call_timeout: std::time::Duration,
    inner: Arc<WsConnectionInner>,
}

/// A serialized JSON-RPC message waiting for the write loop.
struct Outgoing {
    /// The connection the message was made for. Once that one is lost the
    /// message must not reach its successor: the caller has already been
    /// failed, and a subscription id inside it names something else there.
    generation: u64,
    json: String,
}

/// Shared state for a single WS connection, accessed by both the call site
/// and the background connection task.
struct WsConnectionInner {
    /// Pending RPC requests waiting for a response, keyed by internal ID.
    pending: Mutex<HashMap<u32, oneshot::Sender<Result<JsonRpcResponse, UpstreamError>>>>,
    /// Active subscriptions: node-assigned subscription ID -> sender for notification payloads.
    subscriptions: Mutex<HashMap<String, mpsc::UnboundedSender<Box<RawValue>>>>,
    /// Channel for sending serialized JSON-RPC messages to the write loop.
    outgoing: mpsc::Sender<Outgoing>,
    /// Which of the successive connections to the node is the current one;
    /// advances every time a connection is lost. Changed only while holding
    /// both the `pending` and the `subscriptions` locks, so reading it under
    /// either tells reliably which connection an entry there belongs to.
    generation: AtomicU64,
    /// Monotonically increasing ID counter for request/response matching.
    next_id: AtomicU32,
    /// Whether this connection is currently established and ready.
    connected: AtomicBool,
}

impl WsConnection {
    /// Create a new WS connection and start its background reconnection loop.
    /// `pause` takes the whole upstream out of rotation when the endpoint
    /// refuses the connection itself (e.g. a provider's 429 on the handshake).
    pub(super) fn new(label: String, target: WsTarget, pause: PauseHandle) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::channel::<Outgoing>(256);

        let inner = Arc::new(WsConnectionInner {
            pending: Mutex::new(HashMap::new()),
            subscriptions: Mutex::new(HashMap::new()),
            outgoing: outgoing_tx,
            generation: AtomicU64::new(0),
            next_id: AtomicU32::new(IDS_START),
            connected: AtomicBool::new(false),
        });

        let call_timeout = target.call_timeout;
        let bg_inner = Arc::clone(&inner);
        let bg_label = label.clone();
        tokio::spawn(async move {
            connection_loop(bg_label, target, bg_inner, outgoing_rx, pause).await;
        });

        Self {
            label,
            call_timeout,
            inner,
        }
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
        self.exchange(request).await.map(|(_, response)| response)
    }

    /// Makes the call, also telling which connection generation answered it.
    async fn exchange(
        &self,
        request: &JsonRpcRequest,
    ) -> Result<(u64, JsonRpcResponse), UpstreamError> {
        let internal_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        tracing::trace!(connection = %self.label, method = %request.method, internal_id, "WS request");

        let (tx, rx) = oneshot::channel();

        // Register before sending so the read loop can route even very fast responses.
        let generation = {
            let mut pending = self.inner.pending.lock().expect("pending lock poisoned");
            pending.insert(internal_id, tx);
            self.inner.generation.load(Ordering::Relaxed)
        };

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

        let outgoing = Outgoing { generation, json };
        if self.inner.outgoing.send(outgoing).await.is_err() {
            self.remove_pending(internal_id);
            return Err(UpstreamError::Transport(
                "WebSocket connection is closed".into(),
            ));
        }

        let result = tokio::time::timeout(self.call_timeout, rx).await;

        // Always clean up — the entry may already be gone (consumed by the read
        // loop), but remove is idempotent.
        self.remove_pending(internal_id);

        match result {
            Ok(Ok(result)) => {
                tracing::trace!(connection = %self.label, internal_id, ok = result.is_ok(), "WS response received");
                result.map(|response| (generation, response))
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
                    self.call_timeout.as_secs()
                )))
            }
        }
    }

    /// Subscribe to an `eth_subscribe` topic on this specific connection.
    ///
    /// The subscription is tied to this connection and ends when it drops.
    pub(super) async fn subscribe(&self, topic: &str) -> Result<WsSubscription, UpstreamError> {
        // There is no connection to tie the subscription to, and waiting for
        // one risks the request going out only after this caller gave up —
        // the node would then serve a subscription nobody holds or can cancel.
        if !self.is_connected() {
            return Err(UpstreamError::Transport(
                "WebSocket is not connected".into(),
            ));
        }
        let request = JsonRpcRequest::new(0, "eth_subscribe".into(), serde_json::json!([topic]));
        let (generation, response) = self.exchange(&request).await?;

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
            // The connection that granted the id may be lost by now. Its
            // entry would sit among the next connection's subscriptions
            // without ever being fed or closed.
            if self.inner.generation.load(Ordering::Relaxed) != generation {
                return Err(UpstreamError::Transport(
                    "WebSocket disconnected while subscribing".into(),
                ));
            }
            subs.insert(sub_id.clone(), tx);
        }

        tracing::debug!(connection = %self.label, sub_id = %sub_id, topic, "subscription active");
        Ok(WsSubscription {
            id: sub_id,
            generation,
            notifications: rx,
            connection: Arc::clone(&self.inner),
        })
    }

    fn remove_pending(&self, id: u32) {
        let mut pending = self.inner.pending.lock().expect("pending lock poisoned");
        pending.remove(&id);
    }
}

// ─── Subscription handle ────────────────────────────────────────────────────

/// A live `eth_subscribe` subscription on one connection.
///
/// Dropping it cancels the subscription on the node as well (legacy
/// `WsSubscriptionsImpl` did the same on cancel): a high-volume topic such as
/// `newPendingTransactions` would otherwise keep streaming to a connection
/// where nobody listens, until its next reconnect.
pub struct WsSubscription {
    /// Node-assigned subscription id, meaningful only on the connection that
    /// granted it.
    id: String,
    /// The connection generation that granted the id.
    generation: u64,
    notifications: mpsc::UnboundedReceiver<Box<RawValue>>,
    connection: Arc<WsConnectionInner>,
}

impl WsSubscription {
    /// The next notification payload, or `None` once the connection dropped —
    /// the subscription is gone with it and the caller should re-subscribe.
    pub async fn recv(&mut self) -> Option<Box<RawValue>> {
        self.notifications.recv().await
    }
}

impl Drop for WsSubscription {
    fn drop(&mut self) {
        {
            let mut subs = self
                .connection
                .subscriptions
                .lock()
                .expect("subscriptions lock poisoned");
            // The node forgot the subscription together with the connection
            // it was made on, and on the current one the same id may name
            // somebody else's subscription.
            if self.connection.generation.load(Ordering::Relaxed) != self.generation {
                return;
            }
            subs.remove(&self.id);
        }
        let request = JsonRpcRequest::new(
            self.connection.next_id.fetch_add(1, Ordering::Relaxed),
            "eth_unsubscribe".into(),
            serde_json::json!([self.id]),
        );
        // Best-effort: `drop` can't wait for room in the channel, and a lost
        // unsubscribe only costs traffic until the connection is recycled.
        if let Ok(json) = serde_json::to_string(&request) {
            let _ = self.connection.outgoing.try_send(Outgoing {
                generation: self.generation,
                json,
            });
        }
    }
}

// ─── Background connection loop ─────────────────────────────────────────────

/// Runs the persistent WebSocket connection, reconnecting on failure.
async fn connection_loop(
    label: String,
    target: WsTarget,
    state: Arc<WsConnectionInner>,
    mut outgoing_rx: mpsc::Receiver<Outgoing>,
    pause: PauseHandle,
) {
    let mut backoff = BACKOFF_INITIAL;

    loop {
        tracing::info!("{label}: connecting to WebSocket {}", target.url);

        match connect_and_run(&label, &target, &state, &mut outgoing_rx).await {
            Ok(()) => {
                tracing::info!("{label}: WebSocket connection closed normally");
            }
            Err(e) => {
                tracing::warn!("{label}: WebSocket error: {e}");
                if let ConnectionError::Refused(status) = e
                    && is_unavailable_status(status)
                {
                    pause.pause(PauseReason::RateLimited, &e.to_string());
                }
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

/// Why a WS connection ended with an error.
enum ConnectionError {
    /// The endpoint answered the handshake with an HTTP error status instead
    /// of upgrading the connection.
    Refused(u16),
    Failed(String),
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionError::Refused(status) => {
                write!(f, "connect failed: HTTP {status} on WebSocket handshake")
            }
            ConnectionError::Failed(message) => write!(f, "{message}"),
        }
    }
}

impl From<String> for ConnectionError {
    fn from(message: String) -> Self {
        ConnectionError::Failed(message)
    }
}

/// Connect to the WS endpoint and run the read/write loops until disconnection.
async fn connect_and_run(
    label: &str,
    target: &WsTarget,
    state: &Arc<WsConnectionInner>,
    outgoing_rx: &mut mpsc::Receiver<Outgoing>,
) -> Result<(), ConnectionError> {
    // Configured limits only; unset ones keep the tungstenite defaults
    // (16 MiB frame / 64 MiB message).
    let mut ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    if let Some(frame_size) = target.frame_size {
        ws_config = ws_config.max_frame_size(Some(frame_size));
    }
    if let Some(msg_size) = target.msg_size {
        ws_config = ws_config.max_message_size(Some(msg_size));
    }
    let (ws_stream, _) = tokio_tungstenite::connect_async_with_config(
        target.client_request()?,
        Some(ws_config),
        false,
    )
    .await
    .map_err(|e| match &e {
        tungstenite::Error::Http(response) => ConnectionError::Refused(response.status().as_u16()),
        _ => ConnectionError::Failed(format!("connect failed: {e}")),
    })?;

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
                        return Err(ConnectionError::Failed(format!("read error: {e}")));
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
                    Some(message) => {
                        if message.generation != state.generation.load(Ordering::Relaxed) {
                            tracing::trace!("{label}: dropping a message queued for a lost connection");
                            continue;
                        }
                        if let Err(e) = ws_write.send(tungstenite::Message::Text(message.json.into())).await {
                            return Err(ConnectionError::Failed(format!("write error: {e}")));
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
    // Both locks are held across the whole cleanup, so nobody can observe the
    // new generation next to an entry of the old one, or the other way round.
    let mut subs = state
        .subscriptions
        .lock()
        .expect("subscriptions lock poisoned");
    let mut pending = state.pending.lock().expect("pending lock poisoned");
    state.generation.fetch_add(1, Ordering::Relaxed);
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(UpstreamError::Transport(reason.to_string())));
    }
    // Dropping senders closes the receivers, signalling disconnect to subscribers
    subs.clear();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::ethereum::ws_stub::StubNode;
    use crate::upstream::state::UpstreamState;

    fn test_pause() -> PauseHandle {
        PauseHandle::new(
            crate::upstream::id::stub_id().clone(),
            Arc::new(UpstreamState::new()),
        )
    }

    /// With the connection never established the response can't arrive, so the
    /// call must fail at the configured `call_timeout` (`options.timeout`)
    /// instead of waiting forever.
    #[tokio::test]
    async fn call_times_out_at_configured_timeout() {
        let target = WsTarget {
            // Nothing listens here; the background loop keeps retrying while
            // the call sits in the outgoing buffer.
            url: "ws://127.0.0.1:1/".to_string(),
            origin: None,
            basic_auth: None,
            call_timeout: std::time::Duration::from_millis(200),
            frame_size: None,
            msg_size: None,
        };
        let conn = WsConnection::new("test".to_string(), target, test_pause());

        let request = JsonRpcRequest::new(1, "eth_blockNumber".into(), serde_json::json!([]));
        let started = std::time::Instant::now();
        let result = conn.call(&request).await;

        assert!(matches!(result, Err(UpstreamError::Transport(_))));
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn ws_request_has_default_origin_and_no_auth() {
        let target = WsTarget {
            url: "ws://localhost:8546".to_string(),
            origin: None,
            basic_auth: None,
            call_timeout: std::time::Duration::from_secs(60),
            frame_size: None,
            msg_size: None,
        };
        let request = target.client_request().unwrap();
        assert_eq!(request.headers().get("Origin").unwrap(), "http://localhost");
        assert!(request.headers().get("Authorization").is_none());
        assert_eq!(
            request.headers().get("User-Agent").unwrap(),
            crate::upstream::USER_AGENT
        );
    }

    #[test]
    fn ws_request_carries_basic_auth_and_origin() {
        let target = WsTarget {
            url: "ws://localhost:8546".to_string(),
            origin: Some("https://myapp.example.com".to_string()),
            call_timeout: std::time::Duration::from_secs(60),
            frame_size: None,
            msg_size: None,
            basic_auth: Some(BasicAuth {
                username: "user".to_string(),
                password: "pass".to_string(),
            }),
        };
        let request = target.client_request().unwrap();
        assert_eq!(
            request.headers().get("Origin").unwrap(),
            "https://myapp.example.com"
        );
        // echo -n 'user:pass' | base64
        assert_eq!(
            request.headers().get("Authorization").unwrap(),
            "Basic dXNlcjpwYXNz"
        );
    }

    /// A WS server that answers the first JSON-RPC request with a response
    /// padded to roughly 200 KB, so tests can steer it across a size limit.
    /// A WS endpoint that refuses every handshake with the given HTTP status.
    async fn serve_refusing_handshake(status_line: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = [0u8; 4096];
                let _ = stream.read(&mut request).await;
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        url
    }

    fn target_at(url: String) -> WsTarget {
        WsTarget {
            url,
            origin: None,
            basic_auth: None,
            call_timeout: std::time::Duration::from_millis(200),
            frame_size: None,
            msg_size: None,
        }
    }

    /// A provider's 429 on the handshake refuses the whole upstream (its HTTP
    /// endpoint shares the key), so it must pause the upstream, not only make
    /// this connection retry.
    #[tokio::test]
    async fn handshake_refused_with_429_pauses_upstream() {
        let url = serve_refusing_handshake("429 Too Many Requests").await;
        let state = Arc::new(UpstreamState::new());
        let pause = PauseHandle::new(crate::upstream::id::stub_id().clone(), Arc::clone(&state));

        let _conn = WsConnection::new("test".to_string(), target_at(url), pause);

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while state.availability() != UpstreamAvailability::Unavailable {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the upstream should have been paused");
    }

    /// A handshake refused for a reason that says nothing about capacity
    /// (here 404, a wrong path) is left to the reconnect loop and validation.
    #[tokio::test]
    async fn handshake_refused_with_404_does_not_pause() {
        let url = serve_refusing_handshake("404 Not Found").await;
        let state = Arc::new(UpstreamState::new());
        let pause = PauseHandle::new(crate::upstream::id::stub_id().clone(), Arc::clone(&state));

        let _conn = WsConnection::new("test".to_string(), target_at(url), pause);
        // Covers the first attempt and the first reconnect (100ms backoff).
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        assert_eq!(state.availability(), UpstreamAvailability::Ok);
    }

    async fn serve_one_oversized_response() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                if let tungstenite::Message::Text(text) = msg {
                    let req: serde_json::Value = serde_json::from_str(&text).unwrap();
                    let response = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": req["id"],
                        "result": "0x".repeat(100_000),
                    });
                    ws.send(tungstenite::Message::Text(response.to_string().into()))
                        .await
                        .unwrap();
                }
            }
        });
        url
    }

    async fn connected(node: &StubNode) -> WsConnection {
        let conn = WsConnection::new("test".to_string(), target_at(node.url()), test_pause());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !conn.is_connected() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("connected to the stub node");
        conn
    }

    #[tokio::test]
    async fn subscription_receives_node_notifications() {
        let node = StubNode::start().await;
        let conn = connected(&node).await;

        let mut subscription = conn.subscribe("newHeads").await.unwrap();
        node.notify(serde_json::json!({"number": "0x1"}));

        let payload = subscription.recv().await.unwrap();
        assert_eq!(payload.get(), r#"{"number":"0x1"}"#);
    }

    #[tokio::test]
    async fn dropped_subscription_is_cancelled_on_node() {
        let node = StubNode::start().await;
        let conn = connected(&node).await;

        let subscription = conn.subscribe("newHeads").await.unwrap();
        drop(subscription);

        assert_eq!(
            node.requests("eth_unsubscribe", 1).await,
            vec![serde_json::json!(["0x1"])]
        );
    }

    /// The node forgot the subscription together with the connection it was
    /// made on, so there is nothing to cancel — and the stub, like some real
    /// nodes, numbers subscriptions per connection, so the stale id names
    /// the live subscription by then.
    #[tokio::test]
    async fn subscription_of_lost_connection_is_not_cancelled() {
        let node = StubNode::start().await;
        let conn = connected(&node).await;

        let mut stale = conn.subscribe("newHeads").await.unwrap();
        node.disconnect();
        assert!(stale.recv().await.is_none());

        let live = async {
            loop {
                if let Ok(subscription) = conn.subscribe("newHeads").await {
                    return subscription;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        let mut live = tokio::time::timeout(std::time::Duration::from_secs(5), live)
            .await
            .expect("re-subscribed after the reconnect");

        drop(stale);
        // A round trip on the same connection: once it is answered, the node
        // has seen whatever dropping the stale subscription sent before it.
        let probe = JsonRpcRequest::new(1, "eth_chainId".into(), serde_json::json!([]));
        conn.call(&probe).await.unwrap();
        assert!(node.requests("eth_unsubscribe", 0).await.is_empty());

        node.notify(serde_json::json!({"number": "0x2"}));
        assert_eq!(live.recv().await.unwrap().get(), r#"{"number":"0x2"}"#);

        drop(live);
        assert_eq!(
            node.requests("eth_unsubscribe", 1).await,
            vec![serde_json::json!(["0x1"])]
        );
    }

    #[tokio::test]
    async fn subscribe_fails_right_away_while_disconnected() {
        let mut target = target_at("ws://127.0.0.1:1/".to_string());
        target.call_timeout = std::time::Duration::from_secs(60);
        let conn = WsConnection::new("test".to_string(), target, test_pause());

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            conn.subscribe("newHeads"),
        )
        .await
        .expect("no waiting for a response that can't come");
        assert!(matches!(result, Err(UpstreamError::Transport(_))));
    }

    /// The size limits were once parsed but silently ignored: prove they reach
    /// the transport by steering the same oversized response across them. With
    /// the default cap (tungstenite's 64 MiB) it goes through; capped at the
    /// 64 KiB floor tungstenite must reject it, dropping the connection before
    /// a response can arrive, so the call fails instead.
    #[tokio::test]
    async fn msg_size_limit_decides_oversized_response_fate() {
        for (msg_size, expect_ok) in [(None, true), (Some(65_535), false)] {
            let url = serve_one_oversized_response().await;
            let target = WsTarget {
                url,
                origin: None,
                basic_auth: None,
                call_timeout: std::time::Duration::from_millis(500),
                frame_size: None,
                msg_size,
            };
            let conn = WsConnection::new("test".to_string(), target, test_pause());
            let request =
                JsonRpcRequest::new(1, "eth_getBlockByNumber".into(), serde_json::json!([]));
            let result = conn.call(&request).await;
            assert_eq!(
                result.is_ok(),
                expect_ok,
                "msg_size={msg_size:?}: {result:?}"
            );
        }
    }
}
