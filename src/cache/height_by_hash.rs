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

//! In-memory LRU cache mapping block hashes to their heights.
//!
//! This is the reverse of [`super::height_cache::HeightCache`] — useful for
//! quick height lookups when only the block hash is known.

use crate::data::{BlockContainer, BlockId};
use moka::sync::Cache;

/// Default capacity — keeps up to 256 hash→height mappings.
const DEFAULT_MAX_ENTRIES: u64 = 256;

/// LRU cache of `block hash → height` mappings.
pub struct HeightByHashCache {
    inner: Cache<BlockId, u64>,
}

impl HeightByHashCache {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_ENTRIES)
    }

    pub fn with_capacity(max_entries: u64) -> Self {
        Self {
            inner: Cache::new(max_entries),
        }
    }

    /// Look up the height for a given block hash.
    pub fn get(&self, id: &BlockId) -> Option<u64> {
        self.inner.get(id)
    }

    /// Record the hash→height mapping from a block.
    pub fn add(&self, block: &BlockContainer) {
        self.inner.insert(block.hash, block.height);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::Timestamp;

    fn make_block(hash_byte: u8, height: u64) -> BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
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
        let cache = HeightByHashCache::new();
        let block = make_block(1, 100);
        cache.add(&block);
        assert_eq!(cache.get(&block.hash), Some(100));
    }

    #[test]
    fn returns_none_for_missing() {
        let cache = HeightByHashCache::new();
        let id = BlockId::from_bytes([0xff; 32]);
        assert!(cache.get(&id).is_none());
    }
}
