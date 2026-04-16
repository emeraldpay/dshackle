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

//! In-memory LRU cache mapping block heights to block hashes.
//!
//! When a new block replaces an existing entry at the same height (i.e., a
//! chain reorganization), the previous hash is returned so callers can evict
//! the stale block from other caches.

use crate::data::{BlockContainer, BlockId};
use moka::sync::Cache;

/// Default capacity — keeps mappings for up to 512 recent heights.
const DEFAULT_MAX_HEIGHTS: u64 = 512;

/// LRU cache of `height → block hash` mappings.
///
/// Detects chain reorganizations: when a block is inserted at a height that
/// already had a different hash, the old hash is returned.
pub struct HeightCache {
    inner: Cache<u64, BlockId>,
}

impl HeightCache {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_HEIGHTS)
    }

    pub fn with_capacity(max_entries: u64) -> Self {
        Self {
            inner: Cache::new(max_entries),
        }
    }

    /// Look up the block hash at the given height.
    pub fn get(&self, height: u64) -> Option<BlockId> {
        self.inner.get(&height)
    }

    /// Record the block's height→hash mapping.
    ///
    /// Returns the **previous** hash at that height if it was different from the
    /// new one — this signals a reorg and lets the caller evict the replaced
    /// block.
    pub fn add(&self, block: &BlockContainer) -> Option<BlockId> {
        let previous = self.inner.get(&block.height);
        self.inner.insert(block.height, block.hash);
        match previous {
            Some(prev) if prev != block.hash => Some(prev),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::TxId;
    use jiff::Timestamp;

    fn make_block(hash_byte: u8, height: u64) -> BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
        BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: None,
            timestamp: Timestamp::UNIX_EPOCH,
            transaction_hashes: Vec::<TxId>::new(),
            json: None,
        }
    }

    #[test]
    fn stores_and_retrieves_height() {
        let cache = HeightCache::new();
        let block = make_block(1, 100);
        cache.add(&block);
        assert_eq!(cache.get(100), Some(block.hash));
    }

    #[test]
    fn returns_none_for_missing_height() {
        let cache = HeightCache::new();
        assert!(cache.get(999).is_none());
    }

    #[test]
    fn returns_none_when_same_hash_reinserted() {
        let cache = HeightCache::new();
        let block = make_block(1, 100);
        assert!(cache.add(&block).is_none());
        // Re-insert same block — no reorg
        assert!(cache.add(&block).is_none());
    }

    #[test]
    fn detects_reorg() {
        let cache = HeightCache::new();
        let original = make_block(1, 100);
        let replacement = make_block(2, 100);

        cache.add(&original);
        let replaced = cache.add(&replacement);

        assert_eq!(replaced, Some(original.hash));
        assert_eq!(cache.get(100), Some(replacement.hash));
    }
}
