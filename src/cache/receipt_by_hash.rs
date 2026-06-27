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

//! In-memory LRU cache for transaction receipts of recent blocks.

use crate::data::{BlockId, TxContainer, TxId};
use moka::sync::Cache;

/// How many recent blocks worth of receipts to keep.
const DEFAULT_BLOCKS: u64 = 6;

/// Expected transactions (and so receipts) per block — sized for Ethereum.
const TXS_PER_BLOCK: u64 = 300;

/// LRU cache of receipts indexed by their transaction hash.
///
/// A receipt is keyed and located exactly like the transaction it belongs
/// to, so it is held in a [`TxContainer`] too — only the JSON payload
/// differs (the receipt instead of the transaction).
pub struct ReceiptByHashCache {
    inner: Cache<TxId, TxContainer>,
    /// Recency window, in blocks behind the head, of receipts worth caching.
    blocks: u64,
}

impl ReceiptByHashCache {
    pub fn new() -> Self {
        Self::with_window(DEFAULT_BLOCKS)
    }

    pub fn with_window(blocks: u64) -> Self {
        Self {
            inner: Cache::new(blocks * TXS_PER_BLOCK),
            blocks,
        }
    }

    /// Whether a receipt for a block at `height` should be cached when the
    /// chain head is at `current_height`.
    ///
    /// Only receipts near the head are kept: those are the ones clients poll
    /// repeatedly (e.g. waiting for confirmations), while a receipt of a
    /// historical transaction is typically read once — caching it would only
    /// churn the recent entries out.
    pub fn accepts(&self, height: Option<u64>, current_height: Option<u64>) -> bool {
        match (height, current_height) {
            (Some(height), Some(current)) => height <= current && current - height <= self.blocks,
            _ => false,
        }
    }

    pub fn get(&self, id: &TxId) -> Option<TxContainer> {
        self.inner.get(id)
    }

    /// Add a receipt. A receipt without a block is for a pending transaction
    /// and is silently ignored — it changes once the transaction is mined.
    pub fn add(&self, receipt: TxContainer) {
        if receipt.block_hash.is_none() {
            return;
        }
        self.inner.insert(receipt.hash, receipt);
    }

    /// Evict the receipts of the given transactions — the known contents of
    /// a replaced block.
    pub fn evict_all(&self, txs: &[TxId]) {
        for id in txs {
            self.inner.invalidate(id);
        }
    }

    /// Evict every receipt recorded as belonging to the given block.
    ///
    /// Requires a full scan, so it is only the fallback for when the replaced
    /// block itself is no longer cached and its transaction list is unknown.
    pub fn evict_by_block(&self, block: &BlockId) {
        let ids: Vec<TxId> = self
            .inner
            .iter()
            .filter(|(_, receipt)| receipt.block_hash == Some(*block))
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

    fn make_receipt(hash_byte: u8, block_byte: Option<u8>) -> TxContainer {
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
    fn stores_and_reads_receipt() {
        let cache = ReceiptByHashCache::new();
        cache.add(make_receipt(1, Some(10)));
        assert!(cache.get(&tx_id(1)).is_some());
    }

    #[test]
    fn ignores_pending_receipt() {
        let cache = ReceiptByHashCache::new();
        cache.add(make_receipt(1, None));
        assert!(cache.get(&tx_id(1)).is_none());
    }

    #[test]
    fn accepts_recent_heights_only() {
        let cache = ReceiptByHashCache::with_window(6);

        assert!(cache.accepts(Some(100), Some(100)));
        assert!(cache.accepts(Some(94), Some(100)));

        assert!(!cache.accepts(Some(93), Some(100))); // too far behind
        assert!(!cache.accepts(Some(101), Some(100))); // ahead of our head
        assert!(!cache.accepts(None, Some(100)));
        assert!(!cache.accepts(Some(100), None));
    }

    #[test]
    fn evicts_listed_receipts() {
        let cache = ReceiptByHashCache::new();
        cache.add(make_receipt(1, Some(10)));
        cache.add(make_receipt(2, Some(11)));

        cache.evict_all(&[tx_id(1)]);

        assert!(cache.get(&tx_id(1)).is_none());
        assert!(cache.get(&tx_id(2)).is_some());
    }

    #[test]
    fn evicts_by_block_scan() {
        let cache = ReceiptByHashCache::new();
        cache.add(make_receipt(1, Some(10)));
        cache.add(make_receipt(2, Some(10)));
        cache.add(make_receipt(3, Some(11)));

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
}
