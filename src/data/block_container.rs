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

//! Block data container used by the caching layer.
//!
//! A [`BlockContainer`] holds essential block metadata alongside the raw JSON
//! representation. The JSON is expected to contain transaction hashes only (not
//! full transaction objects) — full transactions are cached separately.

use std::sync::Arc;

use super::{BlockId, TxId};

/// Cached block data.
///
/// Stores both parsed metadata (for lookups and indexing) and the original JSON
/// bytes (for serving to callers without re-serialization). The JSON should have
/// transactions represented as hashes, not full objects.
#[derive(Clone, Debug)]
pub struct BlockContainer {
    /// Block hash.
    pub hash: BlockId,
    /// Block height (number).
    pub height: u64,
    /// Parent block hash, if known.
    pub parent_hash: Option<BlockId>,
    /// Block timestamp.
    pub timestamp: jiff::Timestamp,
    /// Transaction hashes included in this block.
    pub transaction_hashes: Vec<TxId>,
    /// Raw JSON bytes of the block (with tx hashes, not full tx bodies).
    /// Wrapped in `Arc` for cheap cloning across cache layers.
    pub json: Option<Arc<[u8]>>,
}

impl BlockContainer {
    /// Returns `true` if this container carries the raw JSON representation.
    pub fn has_json(&self) -> bool {
        self.json.is_some()
    }
}

impl PartialEq for BlockContainer {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for BlockContainer {}

impl std::hash::Hash for BlockContainer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_block(hash_byte: u8, height: u64) -> BlockContainer {
        let mut hash_bytes = [0u8; 32];
        hash_bytes[0] = hash_byte;
        BlockContainer {
            hash: BlockId::from_bytes(hash_bytes),
            height,
            parent_hash: None,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
        }
    }

    #[test]
    fn equality_by_hash() {
        let a = sample_block(1, 100);
        let b = sample_block(1, 200); // same hash, different height
        assert_eq!(a, b);
    }

    #[test]
    fn inequality_by_hash() {
        let a = sample_block(1, 100);
        let b = sample_block(2, 100); // different hash, same height
        assert_ne!(a, b);
    }

    #[test]
    fn has_json_when_present() {
        let mut block = sample_block(1, 100);
        assert!(!block.has_json());
        block.json = Some(Arc::from(b"{}".as_slice()));
        assert!(block.has_json());
    }
}
