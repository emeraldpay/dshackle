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

//! Per-chain merged head.
//!
//! Each upstream tracks its own head; consumers that want "the chain's head"
//! (the gRPC `SubscribeHead`, and later the `newHeads` proxy subscription) need
//! a single merged view. [`MergedHead`] follows every upstream's block stream
//! and re-broadcasts a block whenever it advances the best height seen so far,
//! so subscribers get a monotonically increasing head without per-upstream
//! duplicates.

use crate::data::BlockContainer;
use crate::upstream::head::CurrentHead;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::broadcast;

/// Capacity of the merged-head broadcast channel. Subscribers only care about
/// the latest head, so a small buffer is enough; slow ones simply skip ahead.
const HEAD_CHANNEL_CAPACITY: usize = 16;

/// A merged view of all upstream heads for one chain.
pub struct MergedHead {
    sender: broadcast::Sender<Arc<BlockContainer>>,
}

impl MergedHead {
    /// Build a merged head that follows the given upstream heads. Spawns one
    /// background task per head; they run for the lifetime of the process (the
    /// upstreams' `CurrentHead`s are never dropped).
    pub fn new(heads: Vec<Arc<CurrentHead>>) -> Arc<Self> {
        let (sender, _) = broadcast::channel(HEAD_CHANNEL_CAPACITY);
        let merged = Arc::new(Self {
            sender: sender.clone(),
        });

        // Shared across followers so the "best height" is global to the chain,
        // not per-upstream — a lagging upstream never pushes the head backwards.
        let best_height = Arc::new(AtomicI64::new(-1));
        for head in heads {
            spawn_follower(head, sender.clone(), Arc::clone(&best_height));
        }
        merged
    }

    /// Subscribe to merged head blocks.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<BlockContainer>> {
        self.sender.subscribe()
    }
}

/// Forward blocks from one upstream head, emitting only those that advance the
/// shared best height.
fn spawn_follower(
    head: Arc<CurrentHead>,
    sender: broadcast::Sender<Arc<BlockContainer>>,
    best_height: Arc<AtomicI64>,
) {
    let mut rx = head.subscribe_blocks();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(block) => {
                    let height = block.height as i64;
                    // Emit only when this block is higher than anything seen on
                    // the chain so far. `fetch_max` returns the previous best.
                    if best_height.fetch_max(height, Ordering::Relaxed) < height {
                        // A send error just means no active subscribers.
                        let _ = sender.send(block);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::BlockId;

    fn block(height: u64) -> BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = height as u8;
        BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: None,
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        }
    }

    #[tokio::test]
    async fn forwards_advancing_blocks() {
        let head = Arc::new(CurrentHead::new());
        let merged = MergedHead::new(vec![Arc::clone(&head)]);
        let mut rx = merged.subscribe();

        head.update_with_block(block(10));
        head.update_with_block(block(11));

        assert_eq!(rx.recv().await.unwrap().height, 10);
        assert_eq!(rx.recv().await.unwrap().height, 11);
    }

    #[tokio::test]
    async fn merges_two_upstreams_without_going_backwards() {
        let a = Arc::new(CurrentHead::new());
        let b = Arc::new(CurrentHead::new());
        let merged = MergedHead::new(vec![Arc::clone(&a), Arc::clone(&b)]);
        let mut rx = merged.subscribe();

        a.update_with_block(block(100));
        // Drain 100 first so the shared best height is settled before b reports,
        // making the ordering deterministic.
        assert_eq!(rx.recv().await.unwrap().height, 100);

        // b is behind — its block must not be re-emitted as a new head.
        b.update_with_block(block(98));
        // b catches up past the best — this one should come through.
        b.update_with_block(block(101));

        assert_eq!(rx.recv().await.unwrap().height, 101);
    }
}
