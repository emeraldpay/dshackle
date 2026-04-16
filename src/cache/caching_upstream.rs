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

//! Caching [`RpcUpstream`] wrapper.
//!
//! [`CachingUpstream`] sits in the per-upstream call chain and intercepts
//! block-by-hash requests. On a cache hit the response is served directly;
//! on a miss the request is delegated to the inner upstream and the result
//! is cached for future lookups.
//!
//! Only requests that identify a block **by hash** are cacheable — a block
//! hash is immutable, so the same hash always maps to the same data. Requests
//! by height (e.g. `eth_getBlockByNumber`) are **not** handled here because
//! the block at a given height can change during a reorg. A separate layer
//! above this one is responsible for resolving heights to hashes before the
//! request reaches the cache.
//!
//! Chain-specific logic (which methods are cacheable, how to parse responses)
//! is provided via the [`BlockCacheCodec`] trait, with implementations in
//! [`ethereum_block_cache`](super::ethereum_block_cache) and
//! [`bitcoin_block_cache`](super::bitcoin_block_cache).

use crate::cache::{CacheTag, Caches};
use crate::data::{BlockContainer, BlockId};
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use serde_json::value::RawValue;
use std::sync::Arc;

/// Chain-specific logic for block cache integration.
///
/// Implementations decide which requests are cacheable block-by-hash lookups
/// and how to parse block responses into [`BlockContainer`]s.
pub trait BlockCacheCodec: Send + Sync {
    /// Extract the block hash from a request if it is a cacheable
    /// block-by-hash lookup. Returns `None` for all other requests.
    fn cacheable_block_hash(&self, method: &str, params: &serde_json::Value) -> Option<BlockId>;

    /// Parse a successful response into a [`BlockContainer`] for caching.
    /// Returns `None` if the response cannot be parsed or the method is not
    /// a block request.
    fn parse_block_response(&self, method: &str, raw_json: &str) -> Option<BlockContainer>;
}

/// An [`RpcUpstream`] wrapper that serves block-by-hash requests from the
/// cache when possible, and caches block responses from the delegate on a
/// miss.
pub struct CachingUpstream {
    inner: Arc<dyn RpcUpstream>,
    caches: Arc<Caches>,
    codec: Box<dyn BlockCacheCodec>,
}

impl CachingUpstream {
    pub fn new(
        inner: Arc<dyn RpcUpstream>,
        caches: Arc<Caches>,
        codec: impl BlockCacheCodec + 'static,
    ) -> Self {
        Self {
            inner,
            caches,
            codec: Box::new(codec),
        }
    }

    fn read_from_cache(&self, request_id: u32, hash: &BlockId) -> Option<JsonRpcResponse> {
        let block = self.caches.get_block_by_hash(hash)?;

        // Can only serve from cache if we have the raw JSON
        let json_bytes = block.json.as_ref()?;
        let json_str = std::str::from_utf8(json_bytes).ok()?;
        let raw = RawValue::from_string(json_str.to_owned()).ok()?;

        Some(JsonRpcResponse {
            id: serde_json::Value::from(request_id),
            result: Some(raw),
            error: None,
        })
    }
}

#[async_trait::async_trait]
impl RpcUpstream for CachingUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        let block_hash = self
            .codec
            .cacheable_block_hash(request.method.as_str(), &request.params);

        // Try to serve from cache
        if let Some(ref hash) = block_hash {
            if let Some(response) = self.read_from_cache(request.id, hash) {
                tracing::trace!(
                    upstream = self.inner.id(),
                    method = %request.method,
                    hash = %hash,
                    "block served from cache"
                );
                return Ok(response);
            }
        }

        // Delegate to inner upstream
        let response = self.inner.call(request).await?;

        // Cache the response if it was a cacheable block request with a
        // successful, non-null result
        if block_hash.is_some() && response.is_non_empty_result() {
            if let Some(raw) = &response.result {
                if let Some(block) = self
                    .codec
                    .parse_block_response(request.method.as_str(), raw.get())
                {
                    tracing::trace!(
                        upstream = self.inner.id(),
                        method = %request.method,
                        height = block.height,
                        "caching block response"
                    );
                    self.caches.cache(CacheTag::Requested, block);
                }
            }
        }

        Ok(response)
    }

    fn id(&self) -> &str {
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
