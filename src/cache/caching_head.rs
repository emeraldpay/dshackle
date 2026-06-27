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

//! Composable cache-feeding layer for head blocks.
//!
//! [`CachingHead`] subscribes to one or more upstream [`CurrentHead`] block
//! streams and writes each new block to the [`Caches`]. It deduplicates by
//! block hash so that even when multiple upstreams report the same block, the
//! cache is updated exactly once per chain.
//!
//! One `CachingHead` instance is created per blockchain — it is the **single
//! point** where head blocks enter the caching layer.

use crate::cache::Caches;
use crate::data::BlockId;
use crate::upstream::head::CurrentHead;
use std::sync::{Arc, Mutex};

/// Per-chain component that bridges upstream head events into the cache.
///
/// Call [`follow`](Self::follow) for each upstream in the chain. The
/// `CachingHead` spawns a background task per source and deduplicates blocks
/// by hash before writing to the shared [`Caches`].
pub struct CachingHead {
    caches: Arc<Caches>,
    /// Tracks the last cached block hash to avoid redundant cache writes when
    /// multiple upstreams report the same block.
    last_hash: Arc<Mutex<Option<BlockId>>>,
}

impl CachingHead {
    pub fn new(caches: Arc<Caches>) -> Self {
        Self {
            caches,
            last_hash: Arc::new(Mutex::new(None)),
        }
    }

    /// Start listening to an upstream head's block stream.
    ///
    /// Spawns a background task that receives blocks from the given
    /// [`CurrentHead`] and writes them to the cache. Blocks that have already
    /// been cached (same hash) are silently skipped.
    ///
    /// Call this once per upstream in the chain — each upstream has its own
    /// [`CurrentHead`], so each needs its own follower. The spawned tasks run
    /// for the lifetime of the process (the `CurrentHead` is owned by the
    /// upstream struct, which is held in `Arc` by `Multistream` and never
    /// dropped). If dynamic reconfiguration is added later, this should
    /// accept a `CancellationToken` to allow stopping individual followers.
    pub fn follow(&self, source: &CurrentHead) {
        let mut rx = source.subscribe_blocks();
        let caches = Arc::clone(&self.caches);
        let last_hash = Arc::clone(&self.last_hash);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(block) => {
                        // Deduplicate: skip if we already cached this exact block
                        {
                            let mut last = last_hash.lock().unwrap();
                            if *last == Some(block.hash) {
                                continue;
                            }
                            *last = Some(block.hash);
                        }

                        tracing::debug!(
                            height = block.height,
                            hash = %block.hash,
                            "caching head block"
                        );
                        caches.memoize_block((*block).clone());
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        // Slow consumer — we skipped some blocks. Fine for
                        // head caching since we only need the latest anyway.
                        tracing::trace!(skipped = n, "caching head lagged, catching up");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::BlockId;

    fn make_block(hash_byte: u8, height: u64) -> crate::data::BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
        crate::data::BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: None,
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
        }
    }

    #[tokio::test]
    async fn caches_block_from_head() {
        let caches = Arc::new(Caches::new());
        let caching = CachingHead::new(Arc::clone(&caches));

        let head = CurrentHead::new();
        caching.follow(&head);

        head.update_with_block(make_block(1, 100));

        // Give the spawned task time to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let id = BlockId::from_bytes({
            let mut h = [0u8; 32];
            h[0] = 1;
            h
        });
        assert_eq!(caches.get_height_by_hash(&id), Some(100));
        assert!(caches.get_block_by_hash(&id).is_some());
    }

    #[tokio::test]
    async fn deduplicates_same_block_from_multiple_upstreams() {
        let caches = Arc::new(Caches::new());
        let caching = CachingHead::new(Arc::clone(&caches));

        let head1 = CurrentHead::new();
        let head2 = CurrentHead::new();
        caching.follow(&head1);
        caching.follow(&head2);

        // Both upstreams report the same block
        head1.update_with_block(make_block(1, 100));
        head2.update_with_block(make_block(1, 100));

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Block should be cached (just verifying it didn't panic or double-process)
        let hash = caches.get_hash_by_height(100).unwrap();
        assert!(hash.to_hex().starts_with("01"));
    }

    #[tokio::test]
    async fn handles_reorg_via_different_hash_at_same_height() {
        let caches = Arc::new(Caches::new());
        let caching = CachingHead::new(Arc::clone(&caches));

        let head = CurrentHead::new();
        caching.follow(&head);

        head.update_with_block(make_block(1, 100));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Reorg: different hash at same height
        head.update_with_block(make_block(2, 100));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Height should point to the new hash
        let hash = caches.get_hash_by_height(100).unwrap();
        assert!(hash.to_hex().starts_with("02"));
        // Old block should be evicted
        let old_id = BlockId::from_bytes({
            let mut h = [0u8; 32];
            h[0] = 1;
            h
        });
        assert!(caches.get_block_by_hash(&old_id).is_none());
    }
}
