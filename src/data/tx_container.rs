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

//! Transaction data container used by the caching layer.

use std::sync::Arc;

use super::{BlockId, TxId};

/// Cached transaction data.
///
/// Mirrors [`BlockContainer`](super::BlockContainer): parsed metadata for
/// lookups and indexing plus the raw JSON bytes preserved exactly as the
/// upstream returned them.
#[derive(Clone, Debug)]
pub struct TxContainer {
    /// Transaction hash.
    pub hash: TxId,
    /// Hash of the block that includes this transaction. `None` while the
    /// transaction is pending — a pending transaction has no immutable state
    /// yet (its `blockHash`/`blockNumber` fields change once mined), so it
    /// must not be cached.
    pub block_hash: Option<BlockId>,
    /// Height of the including block, if known.
    pub height: Option<u64>,
    /// Raw JSON bytes of the transaction. Wrapped in `Arc` for cheap cloning
    /// across cache layers.
    pub json: Option<Arc<[u8]>>,
}

impl PartialEq for TxContainer {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for TxContainer {}

impl std::hash::Hash for TxContainer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tx(hash_byte: u8) -> TxContainer {
        let mut hash_bytes = [0u8; 32];
        hash_bytes[0] = hash_byte;
        TxContainer {
            hash: TxId::from_bytes(hash_bytes),
            block_hash: None,
            height: None,
            json: None,
        }
    }

    #[test]
    fn equality_by_hash() {
        let a = sample_tx(1);
        let mut b = sample_tx(1);
        b.height = Some(100); // same hash, different metadata
        assert_eq!(a, b);
    }

    #[test]
    fn inequality_by_hash() {
        assert_ne!(sample_tx(1), sample_tx(2));
    }
}
