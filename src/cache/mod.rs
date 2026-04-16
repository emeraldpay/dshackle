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

//! In-memory caching layer for blockchain data.
//!
//! [`Caches`] coordinates three block-related caches:
//! - **block by hash** — stores full [`BlockContainer`] data keyed by hash
//! - **height → hash** — maps block heights to their hashes (detects reorgs)
//! - **hash → height** — reverse lookup from hash to height
//!
//! When a block at an existing height is replaced (chain reorganization), the
//! previous block is automatically evicted from all caches.

pub mod bitcoin_block_cache;
mod block_by_hash;
pub mod caching_head;
pub mod caching_upstream;
pub mod ethereum_block_cache;
mod height_by_hash;
mod height_cache;

use crate::data::{BlockContainer, BlockId};
use block_by_hash::BlockByHashCache;
pub use bitcoin_block_cache::BitcoinBlockCache;
pub use caching_head::CachingHead;
pub use caching_upstream::CachingUpstream;
pub use ethereum_block_cache::EthereumBlockCache;
use height_by_hash::HeightByHashCache;
use height_cache::HeightCache;

/// How a cached block was obtained — determines caching strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheTag {
    /// Latest data produced by the blockchain head. Cached only in memory
    /// because it may be replaced soon (e.g. by a reorg).
    Latest,
    /// Data explicitly requested by a client. Cached more aggressively — in
    /// memory and (when available) in Redis.
    Requested,
}

/// Central cache coordinator for blockchain block data.
///
/// Manages the in-memory caches and handles eviction on chain reorganizations.
/// Each blockchain (chain) should have its own `Caches` instance.
pub struct Caches {
    blocks_by_hash: BlockByHashCache,
    height_to_hash: HeightCache,
    hash_to_height: HeightByHashCache,
}

impl Caches {
    pub fn new() -> Self {
        Self {
            blocks_by_hash: BlockByHashCache::new(),
            height_to_hash: HeightCache::new(),
            hash_to_height: HeightByHashCache::new(),
        }
    }

    /// Cache a block according to the given tag.
    ///
    /// - [`CacheTag::Latest`] — stores only in memory (short-lived head data).
    /// - [`CacheTag::Requested`] — stores in memory (Redis will be added later).
    ///
    /// In both cases, if the block's height was already occupied by a different
    /// hash, the old block is evicted (reorg handling).
    pub fn cache(&self, tag: CacheTag, block: BlockContainer) {
        match tag {
            CacheTag::Latest => {
                self.memoize_block(block);
            }
            CacheTag::Requested => {
                // For requested blocks we also store in Redis (future).
                // For now, the in-memory path is the same.
                self.memoize_block(block);
            }
        }
    }

    /// Store a block in all in-memory caches.
    ///
    /// If the height was already mapped to a different hash (reorg), the
    /// replaced block is evicted from the block-by-hash cache.
    pub fn memoize_block(&self, block: BlockContainer) {
        self.blocks_by_hash.add(block.clone());
        self.hash_to_height.add(&block);
        if let Some(replaced_hash) = self.height_to_hash.add(&block) {
            tracing::debug!(
                height = block.height,
                old = %replaced_hash,
                new = %block.hash,
                "block replaced at height (reorg), evicting old block"
            );
            self.evict(&replaced_hash);
        }
    }

    /// Remove a block (and its dependent data) from all caches.
    pub fn evict(&self, block_id: &BlockId) {
        self.blocks_by_hash.evict(block_id);
        // Note: we intentionally do NOT evict from hash_to_height here.
        // Even for replaced blocks, the old hash→height mapping remains valid
        // (that hash really was at that height once). This matches the legacy
        // behavior where HeightByHashRedisCache is not evicted on reorgs.
    }

    // ─── Readers ──────────────────────────────────────────────────────────

    /// Look up a cached block by its hash.
    pub fn get_block_by_hash(&self, id: &BlockId) -> Option<BlockContainer> {
        self.blocks_by_hash.get(id)
    }

    /// Look up the block hash at a given height.
    pub fn get_hash_by_height(&self, height: u64) -> Option<BlockId> {
        self.height_to_hash.get(height)
    }

    /// Look up the height for a given block hash.
    pub fn get_height_by_hash(&self, id: &BlockId) -> Option<u64> {
        self.hash_to_height.get(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::TxId;
    use jiff::Timestamp;
    use std::sync::Arc;

    fn make_block(hash_byte: u8, height: u64) -> BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
        let mut parent = [0u8; 32];
        parent[0] = hash_byte.wrapping_sub(1);
        BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: Some(BlockId::from_bytes(parent)),
            timestamp: Timestamp::UNIX_EPOCH,
            transaction_hashes: Vec::<TxId>::new(),
            json: Some(Arc::from(b"{}".as_slice())),
        }
    }

    #[test]
    fn cache_latest_block() {
        let caches = Caches::new();
        let block = make_block(1, 100);

        caches.cache(CacheTag::Latest, block.clone());

        assert_eq!(caches.get_block_by_hash(&block.hash).unwrap().height, 100);
        assert_eq!(caches.get_hash_by_height(100), Some(block.hash));
        assert_eq!(caches.get_height_by_hash(&block.hash), Some(100));
    }

    #[test]
    fn cache_requested_block() {
        let caches = Caches::new();
        let block = make_block(1, 100);

        caches.cache(CacheTag::Requested, block.clone());

        assert!(caches.get_block_by_hash(&block.hash).is_some());
    }

    #[test]
    fn reorg_evicts_old_block() {
        let caches = Caches::new();
        let original = make_block(1, 100);
        let replacement = make_block(2, 100);

        caches.cache(CacheTag::Latest, original.clone());
        caches.cache(CacheTag::Latest, replacement.clone());

        // Old block is gone
        assert!(caches.get_block_by_hash(&original.hash).is_none());
        // New block is present
        assert!(caches.get_block_by_hash(&replacement.hash).is_some());
        // Height points to new hash
        assert_eq!(caches.get_hash_by_height(100), Some(replacement.hash));
    }

    #[test]
    fn reorg_preserves_old_hash_to_height() {
        let caches = Caches::new();
        let original = make_block(1, 100);
        let replacement = make_block(2, 100);

        caches.cache(CacheTag::Latest, original.clone());
        caches.cache(CacheTag::Latest, replacement.clone());

        // The old hash→height mapping is intentionally kept
        assert_eq!(caches.get_height_by_hash(&original.hash), Some(100));
        assert_eq!(caches.get_height_by_hash(&replacement.hash), Some(100));
    }

    #[test]
    fn missing_block_returns_none() {
        let caches = Caches::new();
        let id = BlockId::from_bytes([0xff; 32]);
        assert!(caches.get_block_by_hash(&id).is_none());
        assert!(caches.get_hash_by_height(999).is_none());
        assert!(caches.get_height_by_hash(&id).is_none());
    }

    #[test]
    fn sequential_blocks_all_cached() {
        let caches = Caches::new();
        for i in 0..10u8 {
            let block = make_block(i, i as u64);
            caches.cache(CacheTag::Latest, block);
        }
        for i in 0..10u8 {
            let mut hash = [0u8; 32];
            hash[0] = i;
            let id = BlockId::from_bytes(hash);
            assert!(caches.get_block_by_hash(&id).is_some());
            assert_eq!(caches.get_hash_by_height(i as u64), Some(id));
        }
    }
}
