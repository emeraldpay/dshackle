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
//! requests for data that is immutable once known. On a cache hit the
//! response is served directly; on a miss the request is delegated to the
//! inner upstream and the result is cached for future lookups.
//!
//! Only requests that identify data **by hash** are cacheable — a hash is
//! immutable, so the same hash always maps to the same data. Requests by
//! height (e.g. `eth_getBlockByNumber`) are **not** handled here because the
//! block at a given height can change during a reorg; the
//! [`NormalizingUpstream`](super::NormalizingUpstream) layer above resolves
//! heights to hashes before the request reaches the cache.
//!
//! Four kinds of calls are served (see [`CacheableCall`]):
//! - **block by hash** with transaction hashes only;
//! - **block by hash with full transaction bodies** — assembled from the
//!   cached block plus the individually cached transactions, and decomposed
//!   into those parts when the response comes from the upstream;
//! - **transaction by hash**;
//! - **receipt by transaction hash** — cached only for blocks near the head.
//!
//! Chain-specific logic (which methods are cacheable, how to parse and
//! assemble responses) is provided via the [`CacheCodec`] trait, with
//! implementations in [`ethereum_codec`](super::ethereum_codec) and
//! [`bitcoin_codec`](super::bitcoin_codec).

use crate::cache::{CacheTag, Caches};
use crate::data::{BlockContainer, BlockId, TxContainer, TxId};
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use serde_json::value::RawValue;
use std::sync::Arc;

/// A request for immutable data that the cache can hold.
#[derive(Debug, PartialEq, Eq)]
pub enum CacheableCall {
    /// Block by hash, transactions as hashes only.
    Block(BlockId),
    /// Block by hash with full transaction bodies.
    FullBlock(BlockId),
    /// Single transaction by hash.
    Tx(TxId),
    /// Transaction receipt, by the transaction hash.
    Receipt(TxId),
}

/// Parsed content of an upstream response, ready to be stored.
pub enum CacheUpdate {
    Block(BlockContainer),
    /// A full block is stored as its parts: the block-only container plus
    /// each transaction, so the same data also serves block-only and
    /// transaction lookups.
    FullBlock {
        block: BlockContainer,
        txs: Vec<TxContainer>,
    },
    Tx(TxContainer),
    /// A receipt is located exactly like its transaction, so it is carried
    /// in a [`TxContainer`] holding the receipt JSON.
    Receipt(TxContainer),
}

/// Chain-specific logic for cache integration.
///
/// Implementations decide which requests are cacheable lookups, how to parse
/// responses into containers, and how to assemble a full block from cached
/// parts.
pub trait CacheCodec: Send + Sync {
    /// Identify a cacheable request. Returns `None` for everything else.
    fn classify(&self, method: &str, params: &serde_json::Value) -> Option<CacheableCall>;

    /// Parse a successful response to the given call into cacheable content.
    /// Returns `None` if the response cannot be parsed.
    fn parse_response(&self, call: &CacheableCall, raw_json: &str) -> Option<CacheUpdate>;

    /// Assemble the full-block JSON from a cached block-only container and
    /// its transactions. Returns `None` when the chain does not support it.
    fn rebuild_full_block(&self, block: &BlockContainer, txs: &[TxContainer]) -> Option<String>;
}

/// An [`RpcUpstream`] wrapper that serves hash-keyed lookups from the cache
/// when possible, and caches responses from the delegate on a miss.
pub struct CachingUpstream {
    inner: Arc<dyn RpcUpstream>,
    caches: Arc<Caches>,
    codec: Box<dyn CacheCodec>,
}

impl CachingUpstream {
    pub fn new(
        inner: Arc<dyn RpcUpstream>,
        caches: Arc<Caches>,
        codec: impl CacheCodec + 'static,
    ) -> Self {
        Self {
            inner,
            caches,
            codec: Box::new(codec),
        }
    }

    async fn read_from_cache(
        &self,
        request_id: u32,
        call: &CacheableCall,
    ) -> Option<JsonRpcResponse> {
        match call {
            CacheableCall::Block(hash) => {
                let block = self.caches.read_block_by_hash(hash).await?;
                json_response(request_id, block.json.as_deref()?)
            }
            CacheableCall::FullBlock(hash) => {
                let block = self.caches.read_block_by_hash(hash).await?;
                block.json.as_ref()?;
                // Every transaction must be cached with its JSON — a partial
                // set cannot produce a correct full block, so it's a miss
                let mut txs: Vec<TxContainer> = Vec::with_capacity(block.transaction_hashes.len());
                for id in &block.transaction_hashes {
                    txs.push(self.caches.read_tx_by_hash(id).await?);
                }
                let full = self.codec.rebuild_full_block(&block, &txs)?;
                json_response(request_id, full.as_bytes())
            }
            CacheableCall::Tx(hash) => {
                let tx = self.caches.read_tx_by_hash(hash).await?;
                json_response(request_id, tx.json.as_deref()?)
            }
            CacheableCall::Receipt(hash) => {
                let receipt = self.caches.read_receipt_by_hash(hash).await?;
                json_response(request_id, receipt.json.as_deref()?)
            }
        }
    }

    fn store(&self, update: CacheUpdate) {
        match update {
            CacheUpdate::Block(block) => {
                tracing::trace!(
                    upstream = self.inner.id(),
                    height = block.height,
                    "caching block response"
                );
                self.caches.cache(CacheTag::Requested, block);
            }
            CacheUpdate::FullBlock { block, txs } => {
                tracing::trace!(
                    upstream = self.inner.id(),
                    height = block.height,
                    txs = txs.len(),
                    "caching full block response"
                );
                self.caches.cache(CacheTag::Requested, block);
                for tx in txs {
                    self.caches.cache_tx(CacheTag::Requested, tx);
                }
            }
            CacheUpdate::Tx(tx) => {
                tracing::trace!(
                    upstream = self.inner.id(),
                    hash = %tx.hash,
                    "caching transaction response"
                );
                self.caches.cache_tx(CacheTag::Requested, tx);
            }
            CacheUpdate::Receipt(receipt) => {
                tracing::trace!(
                    upstream = self.inner.id(),
                    hash = %receipt.hash,
                    "caching receipt response"
                );
                self.caches.cache_receipt(
                    CacheTag::Requested,
                    receipt,
                    self.inner.head().current_height(),
                );
            }
        }
    }
}

/// Build a successful response carrying the given JSON bytes as the result.
fn json_response(request_id: u32, json: &[u8]) -> Option<JsonRpcResponse> {
    let json_str = std::str::from_utf8(json).ok()?;
    let raw = RawValue::from_string(json_str.to_owned()).ok()?;
    Some(JsonRpcResponse {
        id: serde_json::Value::from(request_id),
        result: Some(raw),
        error: None,
        provided_signature: None,
    })
}

#[async_trait::async_trait]
impl RpcUpstream for CachingUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        let call = self
            .codec
            .classify(request.method.as_str(), &request.params);

        // Try to serve from cache
        if let Some(ref call) = call {
            if let Some(response) = self.read_from_cache(request.id, call).await {
                tracing::trace!(
                    upstream = self.inner.id(),
                    method = %request.method,
                    call = ?call,
                    "served from cache"
                );
                return Ok(response);
            }
        }

        // Delegate to inner upstream
        let response = self.inner.call(request).await?;

        // Cache the response if it was a cacheable request with a
        // successful, non-null result
        if let Some(call) = call {
            if response.is_non_empty_result() {
                if let Some(raw) = &response.result {
                    if let Some(update) = self.codec.parse_response(&call, raw.get()) {
                        self.store(update);
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::EthereumCacheCodec;
    use crate::upstream::head::CurrentHead;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static MOCK_STATE: std::sync::LazyLock<Arc<UpstreamState>> =
        std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));

    const BLOCK_HASH: &str = "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";
    const TX_A_HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TX_B_HASH: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn full_block_json() -> String {
        format!(
            r#"{{"hash":"{BLOCK_HASH}","number":"0x64","parentHash":"0x787899711b862b77df8d2faa69de664048598265a9f96abf178d341076e200e0","timestamp":"0x65a8b44c","transactions":[{{"hash":"{TX_A_HASH}","blockHash":"{BLOCK_HASH}","blockNumber":"0x64"}},{{"hash":"{TX_B_HASH}","blockHash":"{BLOCK_HASH}","blockNumber":"0x64"}}],"gasUsed":"0x1234"}}"#
        )
    }

    fn receipt_json() -> String {
        format!(
            r#"{{"transactionHash":"{TX_A_HASH}","blockHash":"{BLOCK_HASH}","blockNumber":"0x64","status":"0x1"}}"#
        )
    }

    /// Answers block requests with the configured full block, receipt
    /// requests with its first transaction's receipt, and counts calls.
    struct StubUpstream {
        head: CurrentHead,
        calls: AtomicUsize,
    }

    impl StubUpstream {
        fn new() -> Self {
            Self::at_height(0x64)
        }

        fn at_height(height: u64) -> Self {
            let head = CurrentHead::new();
            head.update(height);
            Self {
                head,
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let result = if request.method.as_str() == "eth_getTransactionReceipt" {
                receipt_json()
            } else {
                full_block_json()
            };
            let raw = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{result}}}"#);
            Ok(serde_json::from_str(&raw).unwrap())
        }
        fn id(&self) -> &str {
            "stub"
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

    fn full_block_request() -> JsonRpcRequest {
        JsonRpcRequest::new(
            1,
            "eth_getBlockByHash".into(),
            serde_json::json!([BLOCK_HASH, true]),
        )
    }

    fn caching(inner: Arc<StubUpstream>) -> CachingUpstream {
        CachingUpstream::new(inner, Arc::new(Caches::new()), EthereumCacheCodec)
    }

    #[tokio::test]
    async fn full_block_cached_after_first_request() {
        let inner = Arc::new(StubUpstream::new());
        let up = caching(Arc::clone(&inner));

        let first = up.call(&full_block_request()).await.unwrap();
        let second = up.call(&full_block_request()).await.unwrap();

        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            first.result.unwrap().get(),
            second.result.unwrap().get(),
            "reconstructed block must match the original response"
        );
    }

    #[tokio::test]
    async fn full_block_serves_block_only_lookup() {
        let inner = Arc::new(StubUpstream::new());
        let up = caching(Arc::clone(&inner));

        up.call(&full_block_request()).await.unwrap();

        let req = JsonRpcRequest::new(
            2,
            "eth_getBlockByHash".into(),
            serde_json::json!([BLOCK_HASH, false]),
        );
        let resp = up.call(&req).await.unwrap();

        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        let v: serde_json::Value = serde_json::from_str(resp.result.unwrap().get()).unwrap();
        assert_eq!(v["transactions"], serde_json::json!([TX_A_HASH, TX_B_HASH]));
    }

    #[tokio::test]
    async fn full_block_serves_transaction_lookup() {
        let inner = Arc::new(StubUpstream::new());
        let up = caching(Arc::clone(&inner));

        up.call(&full_block_request()).await.unwrap();

        let req = JsonRpcRequest::new(
            3,
            "eth_getTransactionByHash".into(),
            serde_json::json!([TX_A_HASH]),
        );
        let resp = up.call(&req).await.unwrap();

        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        let v: serde_json::Value = serde_json::from_str(resp.result.unwrap().get()).unwrap();
        assert_eq!(v["hash"], TX_A_HASH);
    }

    #[tokio::test]
    async fn full_block_with_missing_tx_goes_upstream() {
        let inner = Arc::new(StubUpstream::new());
        let caches = Arc::new(Caches::new());
        let up = CachingUpstream::new(
            Arc::clone(&inner) as _,
            Arc::clone(&caches),
            EthereumCacheCodec,
        );

        // Cache holds the block-only part but not its transactions
        let (block, _) = crate::cache::ethereum_full_block::decompose(&full_block_json()).unwrap();
        caches.cache(CacheTag::Requested, block);

        up.call(&full_block_request()).await.unwrap();

        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    fn receipt_request() -> JsonRpcRequest {
        JsonRpcRequest::new(
            4,
            "eth_getTransactionReceipt".into(),
            serde_json::json!([TX_A_HASH]),
        )
    }

    #[tokio::test]
    async fn recent_receipt_cached_after_first_request() {
        let inner = Arc::new(StubUpstream::new());
        let up = caching(Arc::clone(&inner));

        let first = up.call(&receipt_request()).await.unwrap();
        let second = up.call(&receipt_request()).await.unwrap();

        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.result.unwrap().get(), second.result.unwrap().get());
    }

    #[tokio::test]
    async fn old_receipt_not_cached() {
        // The receipt's block (0x64) is far behind this upstream's head
        let inner = Arc::new(StubUpstream::at_height(0x64 + 100));
        let up = caching(Arc::clone(&inner));

        up.call(&receipt_request()).await.unwrap();
        up.call(&receipt_request()).await.unwrap();

        assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn non_cacheable_request_always_delegates() {
        let inner = Arc::new(StubUpstream::new());
        let up = caching(Arc::clone(&inner));

        let req = JsonRpcRequest::new(1, "eth_getBalance".into(), serde_json::json!(["0x1"]));
        up.call(&req).await.unwrap();
        up.call(&req).await.unwrap();

        assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
    }
}
