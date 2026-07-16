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
//! Supports one or more persistent WebSocket connections to the same endpoint.
//! When multiple connections are configured (via the `connections` field in the
//! WS endpoint config), they are grown gradually and RPC calls are distributed
//! round-robin across connected instances.
//!
//! Subscriptions (`eth_subscribe`) are pinned to a single connection.
//! When that connection drops the subscription channel closes and the caller
//! should re-subscribe.

use super::ws_conn::WsTarget;
use super::ws_pool::WsConnectionPool;
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::{CurrentHead, Head};
use crate::upstream::id::UpstreamId;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use serde_json::value::RawValue;
use std::sync::Arc;
use tokio::sync::mpsc;

/// An Ethereum upstream node accessed over WebSocket JSON-RPC.
///
/// Supports one or more persistent connections (configured via the `connections`
/// field in the WS endpoint config). Multiple connections are grown gradually
/// and RPC calls are distributed round-robin across them.
pub struct EthereumWsUpstream {
    id: UpstreamId,
    pool: Arc<WsConnectionPool>,
    head: Arc<CurrentHead>,
    upstream_state: Arc<UpstreamState>,
}

impl EthereumWsUpstream {
    /// Create a new WS upstream with the given number of connections.
    ///
    /// Connections are established in background tasks and automatically
    /// reconnect with exponential backoff on failure.
    pub fn new(id: UpstreamId, target: WsTarget, connections: u32) -> Self {
        let pool = WsConnectionPool::start(id.clone(), target, connections.max(1));
        let head = Arc::new(CurrentHead::new());
        let upstream_state = Arc::new(UpstreamState::new());
        Self {
            id,
            pool,
            head,
            upstream_state,
        }
    }

    /// Shared reference to this upstream's head height tracker.
    pub fn head_height(&self) -> Arc<CurrentHead> {
        Arc::clone(&self.head)
    }

    /// Subscribe to an `eth_subscribe` topic (e.g. `"newHeads"`).
    ///
    /// The subscription is pinned to a single underlying connection. When that
    /// connection drops, the receiver closes — callers should re-subscribe.
    pub async fn subscribe(
        &self,
        topic: &str,
    ) -> Result<mpsc::UnboundedReceiver<Box<RawValue>>, UpstreamError> {
        let conn = self
            .pool
            .get_connection()
            .ok_or_else(|| UpstreamError::Transport("No WS connections available yet".into()))?;
        conn.subscribe(topic).await
    }
}

#[async_trait::async_trait]
impl RpcUpstream for EthereumWsUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        let conn = self
            .pool
            .get_connection()
            .ok_or_else(|| UpstreamError::Transport("No WS connections available yet".into()))?;
        conn.call(request).await
    }

    fn id(&self) -> &UpstreamId {
        &self.id
    }

    fn availability(&self) -> UpstreamAvailability {
        self.upstream_state.availability()
    }

    fn head(&self) -> &dyn Head {
        self.head.as_ref()
    }

    fn lag(&self) -> Option<u64> {
        self.upstream_state.lag()
    }

    fn state(&self) -> &Arc<UpstreamState> {
        &self.upstream_state
    }
}
