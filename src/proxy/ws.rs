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
//! Serves the same `/<route-id>` endpoints as the HTTP proxy, but over a
//! persistent WebSocket connection. Each text frame is one JSON-RPC request,
//! answered through the shared call path. Ports the plain-call half of the
//! legacy `WebsocketHandler`; `eth_subscribe` / `eth_unsubscribe` arrive with
//! the egress work (3.4).

use super::handler;
use super::protocol::{self, BodyKind};
use crate::upstream::Multistream;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use warp::ws::{Message, WebSocket};

const METHOD_SUBSCRIBE: &str = "eth_subscribe";
const METHOD_UNSUBSCRIBE: &str = "eth_unsubscribe";

/// Serve one accepted WebSocket connection until the client disconnects.
///
/// Requests are processed concurrently and responses are written back as they
/// complete (matching the legacy `flatMap`), so a slow upstream call never
/// blocks later requests on the same connection. A single writer task owns the
/// sink; this is also the seam subscription pushes will use in 3.4.
pub async fn serve(socket: WebSocket, multistream: Arc<Multistream>) {
    let (mut sink, mut stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

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

        let bytes = msg.as_bytes().to_vec();
        let multistream = Arc::clone(&multistream);
        let out_tx = out_tx.clone();
        tokio::spawn(async move {
            if let Some(response) = dispatch(&bytes, &multistream).await {
                let _ = out_tx.send(Message::text(response));
            }
        });
    }

    // Drop the read loop's sender so the writer ends once in-flight responses
    // (whose tasks hold their own clones) have drained.
    drop(out_tx);
    let _ = writer.await;
}

/// Turn one incoming frame into an optional response body.
///
/// Returns `None` for anything the connection should silently swallow — batches
/// (unsupported over WS) and malformed requests — exactly as the legacy proxy
/// does ("that's what other Ethereum servers do").
async fn dispatch(bytes: &[u8], multistream: &Multistream) -> Option<String> {
    // WS carries one request per frame; batches are ignored, not answered.
    if !matches!(protocol::detect_kind(bytes), Ok(BodyKind::Single)) {
        return None;
    }
    let value = protocol::parse_body(bytes).ok()?;
    let req = protocol::validate_item(&value).ok()?;

    match req.method.as_str() {
        // Subscriptions ride on top of this transport in 3.4. Until then reject
        // them explicitly rather than forwarding a meaningless eth_subscribe to
        // an HTTP upstream.
        METHOD_SUBSCRIBE | METHOD_UNSUBSCRIBE => Some(protocol::build_error(
            &req.id,
            -32601,
            "Subscriptions are not yet supported over the proxy".to_string(),
            None,
        )),
        _ => Some(handler::run(multistream, &req).await),
    }
}
