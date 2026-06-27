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

//! WebSocket connection pool that manages multiple [`WsConnection`]s to the
//! same endpoint.
//!
//! Connections are grown gradually — one every 5 seconds, only when all
//! existing connections are healthy (matching the legacy `WsConnectionMultiPool`
//! behavior). RPC calls are distributed round-robin across connected instances;
//! subscriptions are pinned to one connection.

use super::ws_conn::WsConnection;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

/// Delay between adding pool connections, matching the legacy `SCHEDULE_GROW`.
const POOL_GROW_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Give up waiting for existing connections and add the next one anyway.
const POOL_GROW_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// Pool of WebSocket connections to the same endpoint.
///
/// Connections are grown gradually — one every [`POOL_GROW_INTERVAL`], only
/// when all existing connections are healthy. RPC calls are distributed
/// round-robin across connected instances; subscriptions are pinned to one
/// connection.
pub(super) struct WsConnectionPool {
    connections: RwLock<Vec<Arc<WsConnection>>>,
    next_index: AtomicUsize,
}

impl WsConnectionPool {
    /// Create a pool and start growing connections in the background.
    pub(super) fn start(upstream_id: String, url: String, target: u32) -> Arc<Self> {
        let pool = Arc::new(Self {
            connections: RwLock::new(Vec::with_capacity(target as usize)),
            next_index: AtomicUsize::new(0),
        });

        if target > 1 {
            tracing::info!("Upstream {upstream_id}: opening {target} WS connections to {url}");
        }

        let pool_clone = Arc::clone(&pool);
        tokio::spawn(async move {
            grow_pool(pool_clone, upstream_id, url, target).await;
        });

        pool
    }

    /// Pick a connection, preferring ones that are currently connected.
    ///
    /// Returns `None` only when the pool has no connections yet (during
    /// initial startup before the first connection is created).
    pub(super) fn get_connection(&self) -> Option<Arc<WsConnection>> {
        let conns = self.connections.read().expect("connections lock poisoned");
        if conns.is_empty() {
            return None;
        }
        let len = conns.len();
        let start = self.next_index.fetch_add(1, Ordering::Relaxed);

        // Try to find a connected one starting from the round-robin position
        for offset in 0..len {
            let idx = (start + offset) % len;
            if conns[idx].is_connected() {
                return Some(Arc::clone(&conns[idx]));
            }
        }

        // All disconnected — return one anyway (it will fail with a clear transport error
        // rather than a confusing "no connections" error)
        Some(Arc::clone(&conns[start % len]))
    }
}

/// Gradually add connections to the pool until the target is reached.
///
/// Each new connection is added only once all existing ones are connected,
/// matching the legacy behavior where a broken connection pauses growth.
/// After [`POOL_GROW_MAX_WAIT`] the next connection is added regardless,
/// to avoid stalling forever on a flaky endpoint.
async fn grow_pool(pool: Arc<WsConnectionPool>, upstream_id: String, url: String, target: u32) {
    for i in 0..target {
        let label = if target == 1 {
            format!("Upstream {upstream_id}")
        } else {
            format!("Upstream {upstream_id}/{}", i + 1)
        };

        let conn = Arc::new(WsConnection::new(label, url.clone()));
        {
            let mut conns = pool.connections.write().expect("connections lock poisoned");
            conns.push(conn);
        }

        // Wait for existing connections to be healthy before adding the next one
        if i + 1 < target {
            let deadline = tokio::time::Instant::now() + POOL_GROW_MAX_WAIT;
            loop {
                tokio::time::sleep(POOL_GROW_INTERVAL).await;
                let all_connected = {
                    let conns = pool.connections.read().expect("connections lock poisoned");
                    conns.iter().all(|c| c.is_connected())
                };
                if all_connected || tokio::time::Instant::now() >= deadline {
                    break;
                }
            }
        }
    }

    if target > 1 {
        tracing::info!("Upstream {upstream_id}: all {target} WS connections created");
    }
}
