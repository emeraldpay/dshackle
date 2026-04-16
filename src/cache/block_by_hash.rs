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

//! In-memory LRU cache mapping block hashes to their full [`BlockContainer`].

use crate::data::{BlockContainer, BlockId};
use moka::sync::Cache;

/// Default capacity — keeps up to 64 recent blocks in memory.
const DEFAULT_MAX_BLOCKS: u64 = 64;

/// LRU cache of blocks indexed by hash.
pub struct BlockByHashCache {
    inner: Cache<BlockId, BlockContainer>,
}

impl BlockByHashCache {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_BLOCKS)
    }

    pub fn with_capacity(max_entries: u64) -> Self {
        Self {
            inner: Cache::new(max_entries),
        }
    }

    pub fn get(&self, id: &BlockId) -> Option<BlockContainer> {
        self.inner.get(id)
    }

    pub fn add(&self, block: BlockContainer) {
        self.inner.insert(block.hash, block);
    }

    pub fn evict(&self, id: &BlockId) {
        self.inner.invalidate(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::Timestamp;

    fn make_block(byte: u8, height: u64) -> BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = byte;
        BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: None,
            timestamp: Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
        }
    }

    #[test]
    fn stores_and_retrieves() {
        let cache = BlockByHashCache::new();
        let block = make_block(1, 100);
        cache.add(block.clone());
        assert_eq!(cache.get(&block.hash).unwrap().height, 100);
    }

    #[test]
    fn returns_none_for_missing() {
        let cache = BlockByHashCache::new();
        let id = BlockId::from_bytes([0xff; 32]);
        assert!(cache.get(&id).is_none());
    }

    #[test]
    fn evicts_by_id() {
        let cache = BlockByHashCache::new();
        let block = make_block(1, 100);
        cache.add(block.clone());
        cache.evict(&block.hash);
        assert!(cache.get(&block.hash).is_none());
    }

    #[test]
    fn respects_capacity() {
        let cache = BlockByHashCache::with_capacity(2);
        let b1 = make_block(1, 1);
        let b2 = make_block(2, 2);
        let b3 = make_block(3, 3);
        cache.add(b1.clone());
        cache.add(b2.clone());
        cache.add(b3.clone());
        // moka eviction is async — run pending tasks to force it
        cache.inner.run_pending_tasks();
        // At least one of the first two should have been evicted
        let remaining: u64 = [&b1, &b2, &b3]
            .iter()
            .filter(|b| cache.get(&b.hash).is_some())
            .count() as u64;
        assert!(remaining <= 2);
    }
}
