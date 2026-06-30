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

//! Bitcoin block and transaction reading.
//!
//! The data-access layer the fee estimator and the (upcoming) address/balance
//! trackers sit on top of. Ports the legacy `bitcoin/DataReaders`: read a block
//! by height or hash and a transaction by id, consulting the shared cache first
//! and falling back to the node's JSON-RPC (`getblockhash` / `getblock` /
//! `getrawtransaction`).
//!
//! Results are returned as parsed JSON (`serde_json::Value`) — the same
//! `Map<String, Any>` shape the legacy consumers walk for `vin` / `vout` /
//! `vsize` / `tx`. Confirmed blocks and transactions are immutable, so reads are
//! cached write-through (legacy `Tag.REQUESTED`).

use crate::cache::{CacheTag, Caches};
use crate::data::{BlockId, TxContainer, TxId};
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::bitcoin::head::parse_btc_block;
use crate::upstream::egress::ChainAccess;
use serde_json::value::RawValue;
use serde_json::{Value, json};
use std::sync::Arc;

/// Reads Bitcoin blocks and transactions through a chain's upstreams, backed by
/// the shared [`Caches`].
pub struct BitcoinReader {
    access: Arc<dyn ChainAccess>,
    caches: Option<Arc<Caches>>,
}

impl BitcoinReader {
    /// Build a reader over `access` (the chain's routed call surface), caching
    /// reads in `caches` when present.
    pub fn new(access: Arc<dyn ChainAccess>, caches: Option<Arc<Caches>>) -> Self {
        Self { access, caches }
    }

    /// The chain's current best height, or `None` before any upstream reports a
    /// head. Used by the fee estimator to choose its block window.
    pub fn current_height(&self) -> Option<u64> {
        self.access.current_height()
    }

    /// Read the block at `height` (verbosity 1: JSON with transaction ids).
    /// Resolves the hash via `getblockhash`, then reads the block — legacy
    /// `DataReaders.getBlockJson(height)`. A height already mapped in the cache
    /// skips the `getblockhash` round-trip.
    pub async fn block_by_height(&self, height: u64) -> Option<Value> {
        if let Some(hash) = self
            .caches
            .as_ref()
            .and_then(|c| c.get_hash_by_height(height))
            && let Some(block) = self.block_by_hash(&hash).await
        {
            return Some(block);
        }
        let hash = self.fetch_block_hash(height).await?;
        self.block_by_hash(&hash).await
    }

    /// Read the block with the given `hash` (verbosity 1). Legacy
    /// `DataReaders.getBlockJson(hash)`.
    pub async fn block_by_hash(&self, hash: &BlockId) -> Option<Value> {
        if let Some(caches) = &self.caches
            && let Some(cached) = caches
                .read_block_by_hash(hash)
                .await
                .and_then(|block| block.json)
        {
            return serde_json::from_slice(&cached).ok();
        }

        let request = JsonRpcRequest::new(0, "getblock".into(), json!([hash.to_hex(), 1]));
        let raw = self.call_result(&request).await?;
        // Cache the typed block write-through; an unparsable block just isn't
        // cached (the JSON is still returned to the caller).
        if let (Some(caches), Some(block)) = (&self.caches, parse_btc_block(raw.get())) {
            caches.cache(CacheTag::Requested, block);
        }
        serde_json::from_str(raw.get()).ok()
    }

    /// Read transaction `txid` with inputs/outputs decoded
    /// (`getrawtransaction`, verbose). Legacy `DataReaders.getTxJson(txid)`.
    pub async fn tx(&self, txid: &TxId) -> Option<Value> {
        if let Some(caches) = &self.caches
            && let Some(cached) = caches.read_tx_by_hash(txid).await.and_then(|tx| tx.json)
        {
            return serde_json::from_slice(&cached).ok();
        }

        let request =
            JsonRpcRequest::new(0, "getrawtransaction".into(), json!([txid.to_hex(), true]));
        let raw = self.call_result(&request).await?;
        // Only confirmed transactions (those in a block) are cached — a mempool
        // transaction's fields change once mined (legacy `TxMemCache.add` skips
        // `blockId == null`).
        if let Some(caches) = &self.caches
            && let Some(tx) = parse_btc_tx(txid, raw.get())
            && tx.block_hash.is_some()
        {
            caches.cache_tx(CacheTag::Requested, tx);
        }
        serde_json::from_str(raw.get()).ok()
    }

    /// Resolve the block hash at a height via `getblockhash`.
    async fn fetch_block_hash(&self, height: u64) -> Option<BlockId> {
        let request = JsonRpcRequest::new(0, "getblockhash".into(), json!([height]));
        let raw = self.call_result(&request).await?;
        let hash: String = serde_json::from_str(raw.get()).ok()?;
        hash.parse().ok()
    }

    /// Route a request and return its successful result, or `None` on a
    /// transport failure or a JSON-RPC error.
    async fn call_result(&self, request: &JsonRpcRequest) -> Option<Box<RawValue>> {
        let response = self.access.call(request).await.ok()?;
        if response.error.is_some() {
            return None;
        }
        response.result
    }
}

/// Build a [`TxContainer`] from a verbose `getrawtransaction` response. The
/// container is keyed by the requested `txid` (so cache lookups stay coherent
/// for segwit transactions whose witness hash differs); `blockhash` marks it as
/// confirmed. Legacy `ExtractTx`.
fn parse_btc_tx(txid: &TxId, raw_json: &str) -> Option<TxContainer> {
    let value: Value = serde_json::from_str(raw_json).ok()?;
    let block_hash = value
        .get("blockhash")
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok());
    Some(TxContainer {
        hash: *txid,
        block_hash,
        height: None,
        json: Some(Arc::from(raw_json.as_bytes())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::traits::UpstreamError;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A `ChainAccess` that answers canned results keyed by RPC method and
    /// counts calls, so a test can assert cache hits avoid the upstream.
    struct FakeChain {
        responses: Mutex<HashMap<String, Value>>,
        calls: AtomicU32,
    }

    impl FakeChain {
        fn new() -> Self {
            Self {
                responses: Mutex::new(HashMap::new()),
                calls: AtomicU32::new(0),
            }
        }

        fn with(self, method: &str, result: Value) -> Self {
            self.responses
                .lock()
                .unwrap()
                .insert(method.to_string(), result);
            self
        }

        fn call_count(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ChainAccess for FakeChain {
        fn is_syncing(&self) -> bool {
            false
        }
        async fn call(
            &self,
            request: &JsonRpcRequest,
        ) -> Result<JsonRpcResponse, UpstreamError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let result = self
                .responses
                .lock()
                .unwrap()
                .get(request.method.as_str())
                .cloned()
                .unwrap_or(Value::Null);
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                serde_json::to_string(&result).unwrap()
            );
            Ok(serde_json::from_str(&body).unwrap())
        }
    }

    const BLOCK_HASH: &str = "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6";
    const TXID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn block_json() -> Value {
        json!({
            "hash": BLOCK_HASH,
            "height": 800_000,
            "time": 1_690_000_000,
            "tx": [TXID]
        })
    }

    fn tx_json() -> Value {
        json!({
            "txid": TXID,
            "blockhash": BLOCK_HASH,
            "vsize": 140,
            "vin": [{ "txid": "bb", "vout": 0 }],
            "vout": [{ "value": 0.5 }]
        })
    }

    #[tokio::test]
    async fn block_by_height_resolves_hash_then_block() {
        let chain = FakeChain::new()
            .with("getblockhash", json!(BLOCK_HASH))
            .with("getblock", block_json());
        let reader = BitcoinReader::new(Arc::new(chain), None);

        let block = reader.block_by_height(800_000).await.expect("block read");
        assert_eq!(block["height"], 800_000);
        assert_eq!(block["tx"][0], TXID);
    }

    #[tokio::test]
    async fn block_by_height_returns_none_when_hash_unknown() {
        // getblockhash yields null (e.g. height past the tip).
        let chain = FakeChain::new().with("getblock", block_json());
        let reader = BitcoinReader::new(Arc::new(chain), None);
        assert!(reader.block_by_height(999_999_999).await.is_none());
    }

    #[tokio::test]
    async fn tx_decodes_inputs_and_outputs() {
        let chain = FakeChain::new().with("getrawtransaction", tx_json());
        let reader = BitcoinReader::new(Arc::new(chain), None);

        let txid: TxId = TXID.parse().unwrap();
        let tx = reader.tx(&txid).await.expect("tx read");
        assert_eq!(tx["vsize"], 140);
        assert_eq!(tx["vin"][0]["vout"], 0);
        assert_eq!(tx["vout"][0]["value"], 0.5);
    }

    #[tokio::test]
    async fn block_read_is_cached_write_through() {
        let chain = Arc::new(
            FakeChain::new()
                .with("getblockhash", json!(BLOCK_HASH))
                .with("getblock", block_json()),
        );
        let caches = Arc::new(Caches::new());
        let reader = BitcoinReader::new(chain.clone(), Some(caches));

        // First read: getblockhash + getblock.
        reader.block_by_height(800_000).await.expect("first read");
        let after_first = chain.call_count();
        assert_eq!(after_first, 2);

        // Second read at the same height: served from cache, no upstream calls.
        reader.block_by_height(800_000).await.expect("cached read");
        assert_eq!(chain.call_count(), after_first, "expected a cache hit");
    }

    #[tokio::test]
    async fn confirmed_tx_is_cached_but_mempool_tx_is_not() {
        // Confirmed (has blockhash) → cached on second read.
        let confirmed = Arc::new(FakeChain::new().with("getrawtransaction", tx_json()));
        let caches = Arc::new(Caches::new());
        // The block must be known for the tx Redis TTL; here there's no Redis,
        // so memory caching alone is exercised.
        let reader = BitcoinReader::new(confirmed.clone(), Some(caches));
        let txid: TxId = TXID.parse().unwrap();
        reader.tx(&txid).await.expect("first read");
        reader.tx(&txid).await.expect("cached read");
        assert_eq!(confirmed.call_count(), 1, "confirmed tx should be cached");

        // Mempool (no blockhash) → never cached, every read hits the upstream.
        let mut pending_json = tx_json();
        pending_json.as_object_mut().unwrap().remove("blockhash");
        let pending = Arc::new(FakeChain::new().with("getrawtransaction", pending_json));
        let reader = BitcoinReader::new(pending.clone(), Some(Arc::new(Caches::new())));
        reader.tx(&txid).await.expect("first read");
        reader.tx(&txid).await.expect("second read");
        assert_eq!(pending.call_count(), 2, "mempool tx must not be cached");
    }
}
