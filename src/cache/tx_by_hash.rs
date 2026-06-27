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

//! In-memory LRU cache mapping transaction hashes to their [`TxContainer`].

use crate::data::{BlockId, TxContainer, TxId};
use moka::sync::Cache;

/// Default capacity — Ethereum blocks carry roughly 300 transactions,
/// so this keeps about 32 blocks worth of transactions.
const DEFAULT_MAX_TXS: u64 = 300 * 32;

/// LRU cache of transactions indexed by hash.
pub struct TxByHashCache {
    inner: Cache<TxId, TxContainer>,
}

impl TxByHashCache {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_TXS)
    }

    pub fn with_capacity(max_entries: u64) -> Self {
        Self {
            inner: Cache::new(max_entries),
        }
    }

    pub fn get(&self, id: &TxId) -> Option<TxContainer> {
        self.inner.get(id)
    }

    /// Add a transaction. Pending transactions (no block yet) are silently
    /// ignored — their content changes once they are mined.
    pub fn add(&self, tx: TxContainer) {
        if tx.block_hash.is_none() {
            return;
        }
        self.inner.insert(tx.hash, tx);
    }

    /// Evict the given transactions — the known contents of a replaced block.
    pub fn evict_all(&self, txs: &[TxId]) {
        for id in txs {
            self.inner.invalidate(id);
        }
    }

    /// Evict every transaction recorded as included in the given block.
    ///
    /// Requires a full scan, so it is only the fallback for when the replaced
    /// block itself is no longer cached and its transaction list is unknown.
    pub fn evict_by_block(&self, block: &BlockId) {
        let ids: Vec<TxId> = self
            .inner
            .iter()
            .filter(|(_, tx)| tx.block_hash == Some(*block))
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.inner.invalidate(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_tx(hash_byte: u8, block_byte: Option<u8>) -> TxContainer {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
        TxContainer {
            hash: TxId::from_bytes(hash),
            block_hash: block_byte.map(|b| {
                let mut block = [0u8; 32];
                block[0] = b;
                BlockId::from_bytes(block)
            }),
            height: Some(100),
            json: Some(Arc::from(b"{}".as_slice())),
        }
    }

    fn tx_id(hash_byte: u8) -> TxId {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
        TxId::from_bytes(hash)
    }

    #[test]
    fn stores_and_reads_mined_tx() {
        let cache = TxByHashCache::new();
        cache.add(make_tx(1, Some(10)));
        assert!(cache.get(&tx_id(1)).is_some());
    }

    #[test]
    fn ignores_pending_tx() {
        let cache = TxByHashCache::new();
        cache.add(make_tx(1, None));
        assert!(cache.get(&tx_id(1)).is_none());
    }

    #[test]
    fn evicts_listed_txs() {
        let cache = TxByHashCache::new();
        cache.add(make_tx(1, Some(10)));
        cache.add(make_tx(2, Some(10)));
        cache.add(make_tx(3, Some(11)));

        cache.evict_all(&[tx_id(1), tx_id(2)]);

        assert!(cache.get(&tx_id(1)).is_none());
        assert!(cache.get(&tx_id(2)).is_none());
        assert!(cache.get(&tx_id(3)).is_some());
    }

    #[test]
    fn evicts_by_block_scan() {
        let cache = TxByHashCache::new();
        cache.add(make_tx(1, Some(10)));
        cache.add(make_tx(2, Some(10)));
        cache.add(make_tx(3, Some(11)));

        // moka applies inserts asynchronously; force them through so the
        // scan sees all entries
        cache.inner.run_pending_tasks();

        let mut block = [0u8; 32];
        block[0] = 10;
        cache.evict_by_block(&BlockId::from_bytes(block));

        assert!(cache.get(&tx_id(1)).is_none());
        assert!(cache.get(&tx_id(2)).is_none());
        assert!(cache.get(&tx_id(3)).is_some());
    }

    #[test]
    fn respects_capacity() {
        let cache = TxByHashCache::with_capacity(2);
        for i in 1..=3u8 {
            cache.add(make_tx(i, Some(10)));
        }
        cache.inner.run_pending_tasks();
        let present = (1..=3u8)
            .filter(|i| cache.get(&tx_id(*i)).is_some())
            .count();
        assert!(present <= 2);
    }
}
