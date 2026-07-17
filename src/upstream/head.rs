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

//! Head tracking for blockchain upstreams.
//!
//! A [`Head`] represents the current tip of the chain as seen by an upstream.
//! [`CurrentHead`] tracks both the height (atomically, for lag/status) and
//! broadcasts full [`BlockContainer`] data to downstream consumers like
//! [`CachingHead`](crate::cache::CachingHead).

use crate::data::{BlockContainer, BlockId};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Sentinel value indicating "no height known yet" (stored in the atomic).
const NO_HEIGHT: i64 = -1;

/// Capacity for the block broadcast channel. Slow receivers that fall behind
/// this many blocks will miss intermediate blocks — acceptable because we
/// only care about the latest head.
const BLOCK_CHANNEL_CAPACITY: usize = 16;

/// Current chain tip as observed by an upstream.
pub trait Head: Send + Sync {
    /// Returns the latest known block height, or `None` if not yet available.
    fn current_height(&self) -> Option<u64>;
}

/// A `Head` that has no data yet — used as a placeholder until real head
/// tracking is wired up.
pub struct NoHead;

impl Head for NoHead {
    fn current_height(&self) -> Option<u64> {
        None
    }
}

/// Thread-safe head tracker that stores the current height and broadcasts
/// new blocks to subscribers.
///
/// Shared between the background polling task (writer) and the status
/// reporter / cache layer (readers). Each upstream has its own `CurrentHead`;
/// per-chain aggregation happens in [`CachingHead`](crate::cache::CachingHead).
pub struct CurrentHead {
    height: AtomicI64,
    last_hash: Mutex<Option<BlockId>>,
    block_sender: broadcast::Sender<Arc<BlockContainer>>,
}

impl CurrentHead {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BLOCK_CHANNEL_CAPACITY);
        Self {
            height: AtomicI64::new(NO_HEIGHT),
            last_hash: Mutex::new(None),
            block_sender: tx,
        }
    }

    /// Update with a full block. Advances the tracked height and broadcasts
    /// the block to all subscribers.
    ///
    /// A repeat of the current block is not re-broadcast (the legacy heads'
    /// `distinctUntilChanged`): an HTTP head poller reports the same block on
    /// every cycle, and re-announcing it downstream would make two disagreeing
    /// upstreams flip the merged head back and forth on every poll.
    pub fn update_with_block(&self, block: BlockContainer) {
        self.height
            .fetch_max(block.height as i64, Ordering::Relaxed);
        {
            let mut last = self.last_hash.lock().expect("head hash lock poisoned");
            if *last == Some(block.hash) {
                return;
            }
            *last = Some(block.hash);
        }
        // Ignore send errors — just means no active subscribers yet
        let _ = self.block_sender.send(Arc::new(block));
    }

    /// Update the tracked height only (no block data to broadcast).
    ///
    /// Used as a fallback when the head poller can only determine the height
    /// but not the full block. Only accepts forward progress — a lower height
    /// is silently ignored.
    pub fn update(&self, new_height: u64) {
        self.height.fetch_max(new_height as i64, Ordering::Relaxed);
    }

    /// Subscribe to block events from this head.
    pub fn subscribe_blocks(&self) -> broadcast::Receiver<Arc<BlockContainer>> {
        self.block_sender.subscribe()
    }
}

impl Head for CurrentHead {
    fn current_height(&self) -> Option<u64> {
        let h = self.height.load(Ordering::Relaxed);
        if h < 0 { None } else { Some(h as u64) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::BlockId;

    fn make_block(height: u64) -> BlockContainer {
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

    #[test]
    fn starts_with_no_height() {
        let h = CurrentHead::new();
        assert_eq!(h.current_height(), None);
    }

    #[test]
    fn tracks_height_updates() {
        let h = CurrentHead::new();
        h.update(100);
        assert_eq!(h.current_height(), Some(100));
        h.update(200);
        assert_eq!(h.current_height(), Some(200));
    }

    #[test]
    fn ignores_lower_height() {
        let h = CurrentHead::new();
        h.update(200);
        h.update(100);
        assert_eq!(h.current_height(), Some(200));
    }

    #[test]
    fn update_with_block_advances_height() {
        let h = CurrentHead::new();
        h.update_with_block(make_block(42));
        assert_eq!(h.current_height(), Some(42));
    }

    #[tokio::test]
    async fn subscribers_receive_blocks() {
        let h = CurrentHead::new();
        let mut rx = h.subscribe_blocks();

        h.update_with_block(make_block(10));

        let block = rx.recv().await.unwrap();
        assert_eq!(block.height, 10);
    }

    #[tokio::test]
    async fn repeated_block_is_broadcast_once() {
        // An HTTP poller reports the same head on every cycle; only the first
        // report may reach subscribers.
        let h = CurrentHead::new();
        let mut rx = h.subscribe_blocks();

        h.update_with_block(make_block(10));
        h.update_with_block(make_block(10));
        h.update_with_block(make_block(11));

        assert_eq!(rx.recv().await.unwrap().height, 10);
        assert_eq!(rx.recv().await.unwrap().height, 11);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn multiple_blocks_received_in_order() {
        let h = CurrentHead::new();
        let mut rx = h.subscribe_blocks();

        h.update_with_block(make_block(1));
        h.update_with_block(make_block(2));
        h.update_with_block(make_block(3));

        assert_eq!(rx.recv().await.unwrap().height, 1);
        assert_eq!(rx.recv().await.unwrap().height, 2);
        assert_eq!(rx.recv().await.unwrap().height, 3);
    }
}
