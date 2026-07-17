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

//! Reorg diffing of the chain's announced head stream.
//!
//! `eth_subscribe("logs")` must re-emit the logs of a reorged-away block with
//! `removed: true`, so the plain stream of announced heads is not enough to
//! describe the chain. [`BlockWindow`] keeps a short history of the announced
//! blocks and turns each new one into [`BlockUpdate`]s: a block landing on an
//! already-seen height retracts the previous occupant (`Drop`) before
//! announcing itself (`New`). Ports the detection part of the legacy
//! `ConnectBlockUpdates`, which diffed the merged head's stream the same way.

use crate::data::BlockContainer;
use std::collections::VecDeque;
use std::sync::Arc;

/// How many recent blocks to keep for reorg detection. Matches the legacy
/// `ConnectBlockUpdates.HISTORY_LIMIT` (6 * 3); a reorg deeper than this
/// window goes undetected, same as in the legacy implementation.
pub const WINDOW_LIMIT: usize = 18;

/// A change to the canonical chain, derived from the announced head stream.
///
/// On a reorg the `Drop` for the replaced block is always sent before the
/// `New` for its replacement, so subscribers can retract before re-announcing.
#[derive(Clone)]
pub enum BlockUpdate {
    /// The block joined the canonical chain.
    New(Arc<BlockContainer>),
    /// The block was replaced by another one at the same height.
    Drop(Arc<BlockContainer>),
}

/// Sliding window over the announced head blocks, oldest first.
pub struct BlockWindow {
    blocks: VecDeque<Arc<BlockContainer>>,
}

impl BlockWindow {
    /// An empty window; it fills up as blocks are [applied](Self::apply).
    pub fn new() -> Self {
        Self {
            blocks: VecDeque::with_capacity(WINDOW_LIMIT),
        }
    }

    /// Diff one announced block against the window and update it.
    ///
    /// The input is the already-chosen head stream (the merged head decides
    /// what is canonical), so every unseen block is announced as `New`; the
    /// window's only job is to spot a height being re-announced with a
    /// different hash and retract the previous block first.
    pub fn apply(&mut self, block: Arc<BlockContainer>) -> Vec<BlockUpdate> {
        if self.blocks.iter().any(|b| b.hash == block.hash) {
            return Vec::new();
        }
        let dropped = self
            .blocks
            .iter()
            .position(|b| b.height == block.height)
            .and_then(|pos| self.blocks.remove(pos));
        // Newest at the back regardless of height, so every announced block —
        // including a reorg replacement — stays diffable for a full window
        // after its announcement (legacy re-appended replacements the same
        // way; keeping them in the old slot would age them out early).
        self.blocks.push_back(Arc::clone(&block));
        if self.blocks.len() > WINDOW_LIMIT {
            self.blocks.pop_front();
        }
        match dropped {
            Some(old) => vec![BlockUpdate::Drop(old), BlockUpdate::New(block)],
            None => vec![BlockUpdate::New(block)],
        }
    }
}

impl Default for BlockWindow {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::BlockId;

    fn block(height: u64, hash_byte: u8) -> Arc<BlockContainer> {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
        Arc::new(BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: None,
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        })
    }

    fn is_new(update: &BlockUpdate, hash_byte: u8) -> bool {
        matches!(update, BlockUpdate::New(b) if b.hash == block(0, hash_byte).hash)
    }

    fn is_drop(update: &BlockUpdate, hash_byte: u8) -> bool {
        matches!(update, BlockUpdate::Drop(b) if b.hash == block(0, hash_byte).hash)
    }

    #[test]
    fn unseen_blocks_are_new() {
        let mut window = BlockWindow::new();
        let updates = window.apply(block(100, 1));
        assert_eq!(updates.len(), 1);
        assert!(is_new(&updates[0], 1));
        let updates = window.apply(block(101, 2));
        assert_eq!(updates.len(), 1);
        assert!(is_new(&updates[0], 2));
    }

    #[test]
    fn duplicate_hash_produces_nothing() {
        let mut window = BlockWindow::new();
        window.apply(block(100, 1));
        assert!(window.apply(block(100, 1)).is_empty());
    }

    #[test]
    fn same_height_replacement_drops_before_new() {
        let mut window = BlockWindow::new();
        window.apply(block(100, 1));
        window.apply(block(101, 2));

        let updates = window.apply(block(100, 3));
        assert_eq!(updates.len(), 2);
        assert!(is_drop(&updates[0], 1));
        assert!(is_new(&updates[1], 3));

        // The replacement took the old block's slot: replacing it again
        // retracts the first replacement, not the original.
        let updates = window.apply(block(100, 4));
        assert!(is_drop(&updates[0], 3));
        assert!(is_new(&updates[1], 4));
    }

    #[test]
    fn replacement_stays_diffable_for_a_full_window() {
        let mut window = BlockWindow::new();
        for i in 0..WINDOW_LIMIT as u64 {
            window.apply(block(100 + i, i as u8 + 1));
        }
        // Replace the oldest tracked height: the replacement must be treated
        // as the newest entry, not inherit the replaced block's age.
        window.apply(block(100, 100));
        for i in 0..(WINDOW_LIMIT as u64 - 1) {
            window.apply(block(200 + i, 101 + i as u8));
        }
        let updates = window.apply(block(100, 250));
        assert_eq!(updates.len(), 2);
        assert!(is_drop(&updates[0], 100));
        assert!(is_new(&updates[1], 250));
    }

    #[test]
    fn reorg_deeper_than_window_is_a_plain_new() {
        let mut window = BlockWindow::new();
        for i in 0..(WINDOW_LIMIT as u64 + 1) {
            window.apply(block(100 + i, i as u8 + 1));
        }
        // Height 100 was evicted: its replacement can't retract anything.
        let updates = window.apply(block(100, 200));
        assert_eq!(updates.len(), 1);
        assert!(is_new(&updates[0], 200));
    }
}
