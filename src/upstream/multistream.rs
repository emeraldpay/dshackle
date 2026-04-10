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

//! Round-robin aggregation of multiple upstreams serving the same blockchain.
//!
//! When several upstreams are configured for a single chain, `Multistream`
//! distributes requests across them in a simple round-robin fashion. Future
//! iterations may add health-aware routing, lag observation, and capability
//! aggregation (see the legacy `Multistream.kt`).

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Distributes JSON-RPC requests across multiple upstreams using round-robin.
pub struct Multistream {
    upstreams: Vec<Arc<dyn RpcUpstream>>,
    next: AtomicUsize,
}

impl Multistream {
    pub fn new(upstreams: Vec<Arc<dyn RpcUpstream>>) -> Self {
        assert!(!upstreams.is_empty(), "Multistream requires at least one upstream");
        Self {
            upstreams,
            next: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for Multistream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.upstreams.len();
        self.upstreams[idx].call(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    struct MockUpstream {
        calls: AtomicU32,
        label: String,
    }

    impl MockUpstream {
        fn new(label: &str) -> Self {
            Self {
                calls: AtomicU32::new(0),
                label: label.to_string(),
            }
        }

        fn call_count(&self) -> u32 {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for MockUpstream {
        async fn call(&self, _request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let raw = format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":"{}"}}"#,
                self.label
            );
            Ok(serde_json::from_str(&raw).unwrap())
        }
    }

    fn dummy_request() -> JsonRpcRequest {
        JsonRpcRequest::new(1, "eth_blockNumber".into(), serde_json::json!([]))
    }

    #[tokio::test]
    async fn distributes_requests_round_robin() {
        let a = Arc::new(MockUpstream::new("a"));
        let b = Arc::new(MockUpstream::new("b"));
        let c = Arc::new(MockUpstream::new("c"));

        let ms = Multistream::new(vec![a.clone(), b.clone(), c.clone()]);

        for _ in 0..6 {
            ms.call(&dummy_request()).await.unwrap();
        }

        assert_eq!(a.call_count(), 2);
        assert_eq!(b.call_count(), 2);
        assert_eq!(c.call_count(), 2);
    }

    #[tokio::test]
    async fn single_upstream_always_called() {
        let a = Arc::new(MockUpstream::new("only"));
        let ms = Multistream::new(vec![a.clone()]);

        for _ in 0..3 {
            ms.call(&dummy_request()).await.unwrap();
        }

        assert_eq!(a.call_count(), 3);
    }

    #[test]
    #[should_panic(expected = "at least one upstream")]
    fn panics_on_empty() {
        let _ms = Multistream::new(vec![]);
    }
}
