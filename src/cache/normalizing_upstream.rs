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

//! Normalizing [`RpcUpstream`] wrapper.
//!
//! [`NormalizingUpstream`] rewrites requests that reference a block by a
//! _mutable_ reference (a tag like `latest`, or a height) into an equivalent
//! request using the _immutable_ block hash, when the hash is already known
//! from the head tracker and the height→hash cache.
//!
//! Rewriting matters for two reasons:
//! - **Caching** — [`CachingUpstream`](super::CachingUpstream) below this
//!   layer only serves hash-keyed requests, since only those have an
//!   immutable key. Resolving a height to a hash up front turns a request
//!   that could never be cached into one that can.
//! - **Routing** — a by-hash request returns the same data no matter which
//!   upstream executes it (or retries it), while `latest` may differ between
//!   upstreams that are not equally synced.
//!
//! This is the Rust port of the legacy `NormalizingReader`. The legacy
//! version also reconstructed full-transaction blocks from separately cached
//! transactions; here that lives in the caching layer instead (see
//! [`ethereum_full_block`](super::ethereum_full_block)).
//!
//! Chain-specific rewrite rules are provided via the [`RequestNormalizer`]
//! trait, with the Ethereum implementation in
//! [`ethereum_normalizer`](super::ethereum_normalizer).

use crate::cache::Caches;
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::id::UpstreamId;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// Chain-specific request rewrite rules.
///
/// Implementations decide which requests can be rewritten into a more
/// cacheable / deterministic form, using the upstream's current head and the
/// chain caches to resolve mutable block references.
pub trait RequestNormalizer: Send + Sync {
    /// Rewrite the request into a normalized equivalent, or return `None`
    /// when the request should be passed through unchanged.
    ///
    /// A normalized request must ask for the same data as the original — the
    /// caller may still fall back to the original if the normalized form
    /// fails, so the rewrite has to be safe, not necessarily complete.
    fn normalize(
        &self,
        request: &JsonRpcRequest,
        head: &dyn Head,
        caches: &Caches,
    ) -> Option<JsonRpcRequest>;
}

/// An [`RpcUpstream`] wrapper that rewrites block requests to their
/// normalized (hash-based) form before passing them down the chain.
pub struct NormalizingUpstream {
    inner: Arc<dyn RpcUpstream>,
    caches: Arc<Caches>,
    normalizer: Box<dyn RequestNormalizer>,
}

impl NormalizingUpstream {
    pub fn new(
        inner: Arc<dyn RpcUpstream>,
        caches: Arc<Caches>,
        normalizer: impl RequestNormalizer + 'static,
    ) -> Self {
        Self {
            inner,
            caches,
            normalizer: Box::new(normalizer),
        }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for NormalizingUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        let normalized = self
            .normalizer
            .normalize(request, self.inner.head(), &self.caches);

        let Some(normalized) = normalized else {
            return self.inner.call(request).await;
        };

        tracing::trace!(
            upstream = %self.inner.id(),
            original = %request.method,
            normalized = %normalized.method,
            "request normalized"
        );

        match self.inner.call(&normalized).await {
            Ok(response) if response.is_non_empty_result() => Ok(response),
            // The rewrite was based on possibly stale knowledge: a reorg may
            // have evicted the resolved hash, or a method filter below may
            // reject the rewritten method. The original request is the source
            // of truth, so normalization failures must never surface — retry
            // with the request as the client sent it.
            _ => self.inner.call(request).await,
        }
    }

    fn id(&self) -> &UpstreamId {
        self.inner.id()
    }

    fn availability(&self) -> UpstreamAvailability {
        self.inner.availability()
    }

    fn head(&self) -> &dyn Head {
        self.inner.head()
    }

    fn lag(&self) -> Option<u64> {
        self.inner.lag()
    }

    fn state(&self) -> &Arc<UpstreamState> {
        self.inner.state()
    }

    fn allows_method(&self, method: &RpcMethod) -> bool {
        self.inner.allows_method(method)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CacheTag;
    use crate::data::{BlockContainer, BlockId};
    use crate::upstream::head::CurrentHead;
    use std::sync::Mutex;

    static MOCK_STATE: std::sync::LazyLock<Arc<UpstreamState>> =
        std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));

    /// Records every received request and answers by method name:
    /// `eth_getBlockByHash` → configurable, everything else → a block object.
    struct StubUpstream {
        head: CurrentHead,
        by_hash_response: &'static str,
        by_hash_fails: bool,
        calls: Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl StubUpstream {
        fn new(height: Option<u64>) -> Self {
            let head = CurrentHead::new();
            if let Some(h) = height {
                head.update(h);
            }
            Self {
                head,
                by_hash_response: r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x64"}}"#,
                by_hash_fails: false,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn methods_called(&self) -> Vec<String> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|(m, _)| m.clone())
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            self.calls
                .lock()
                .unwrap()
                .push((request.method.as_str().to_string(), request.params.clone()));
            if request.method.as_str() == "eth_getBlockByHash" {
                if self.by_hash_fails {
                    return Err(UpstreamError::MethodNotAllowed(
                        request.method.as_str().to_string(),
                    ));
                }
                return Ok(serde_json::from_str(self.by_hash_response).unwrap());
            }
            let raw = r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x64","from":"number"}}"#;
            Ok(serde_json::from_str(raw).unwrap())
        }
        fn id(&self) -> &UpstreamId {
            crate::upstream::id::stub_id()
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::Ok
        }
        fn head(&self) -> &dyn Head {
            &self.head
        }
        fn lag(&self) -> Option<u64> {
            None
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &MOCK_STATE
        }
    }

    /// Rewrites `eth_getBlockByNumber` to `eth_getBlockByHash` using the
    /// hash cached for the upstream's head height.
    struct HeadHashNormalizer;

    impl RequestNormalizer for HeadHashNormalizer {
        fn normalize(
            &self,
            request: &JsonRpcRequest,
            head: &dyn Head,
            caches: &Caches,
        ) -> Option<JsonRpcRequest> {
            if request.method.as_str() != "eth_getBlockByNumber" {
                return None;
            }
            let hash = caches.get_hash_by_height(head.current_height()?)?;
            Some(JsonRpcRequest::new(
                request.id,
                "eth_getBlockByHash".into(),
                serde_json::json!([hash.to_hex_prefixed(), false]),
            ))
        }
    }

    fn caches_with_block(height: u64) -> (Arc<Caches>, BlockId) {
        let caches = Arc::new(Caches::new());
        let hash = BlockId::from_bytes([7u8; 32]);
        caches.cache(
            CacheTag::Latest,
            BlockContainer {
                hash,
                height,
                parent_hash: None,
                total_difficulty: alloy::primitives::U256::ZERO,
                timestamp: jiff::Timestamp::UNIX_EPOCH,
                transaction_hashes: vec![],
                json: None,
                header_json: None,
            },
        );
        (caches, hash)
    }

    fn block_by_number_request() -> JsonRpcRequest {
        JsonRpcRequest::new(
            1,
            "eth_getBlockByNumber".into(),
            serde_json::json!(["latest", false]),
        )
    }

    #[tokio::test]
    async fn sends_normalized_request_to_inner() {
        let (caches, hash) = caches_with_block(100);
        let inner = Arc::new(StubUpstream::new(Some(100)));
        let up = NormalizingUpstream::new(Arc::clone(&inner) as _, caches, HeadHashNormalizer);

        let resp = up.call(&block_by_number_request()).await.unwrap();

        assert!(resp.is_non_empty_result());
        let calls = inner.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "eth_getBlockByHash");
        assert_eq!(calls[0].1[0], hash.to_hex_prefixed());
    }

    #[tokio::test]
    async fn passes_through_when_not_normalized() {
        let inner = Arc::new(StubUpstream::new(None));
        let up = NormalizingUpstream::new(
            Arc::clone(&inner) as _,
            Arc::new(Caches::new()),
            HeadHashNormalizer,
        );

        let req = JsonRpcRequest::new(1, "eth_getBalance".into(), serde_json::json!(["0xdead"]));
        up.call(&req).await.unwrap();

        assert_eq!(inner.methods_called(), vec!["eth_getBalance"]);
    }

    #[tokio::test]
    async fn falls_back_to_original_on_empty_result() {
        let (caches, _) = caches_with_block(100);
        let mut stub = StubUpstream::new(Some(100));
        stub.by_hash_response = r#"{"jsonrpc":"2.0","id":1,"result":null}"#;
        let inner = Arc::new(stub);
        let up = NormalizingUpstream::new(Arc::clone(&inner) as _, caches, HeadHashNormalizer);

        let resp = up.call(&block_by_number_request()).await.unwrap();

        assert!(resp.is_non_empty_result());
        assert_eq!(
            inner.methods_called(),
            vec!["eth_getBlockByHash", "eth_getBlockByNumber"]
        );
    }

    #[tokio::test]
    async fn falls_back_to_original_on_error() {
        let (caches, _) = caches_with_block(100);
        let mut stub = StubUpstream::new(Some(100));
        stub.by_hash_fails = true;
        let inner = Arc::new(stub);
        let up = NormalizingUpstream::new(Arc::clone(&inner) as _, caches, HeadHashNormalizer);

        let resp = up.call(&block_by_number_request()).await.unwrap();

        assert!(resp.is_non_empty_result());
        assert_eq!(
            inner.methods_called(),
            vec!["eth_getBlockByHash", "eth_getBlockByNumber"]
        );
    }

    #[tokio::test]
    async fn delegates_identity() {
        let inner = Arc::new(StubUpstream::new(None));
        let up = NormalizingUpstream::new(
            Arc::clone(&inner) as _,
            Arc::new(Caches::new()),
            HeadHashNormalizer,
        );

        assert_eq!(up.id(), "stub");
        assert_eq!(up.availability(), UpstreamAvailability::Ok);
        assert!(up.allows_method(&"eth_getBalance".into()));
    }
}
