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
//! Each upstream tracks its own head, but the upstreams may disagree.
//! [`MergedHead`] follows every upstream (health does not gate participation —
//! a stalled upstream simply stops contributing, it can never stall the chain)
//! and keeps a short window of chosen blocks per height, resolving conflicts
//! by [`MergeOrder`]: cumulative difficulty for PoW, upstream priority for PoS
//! (equal priorities: the latest announcement wins). After every accepted
//! block the current tip is re-announced, deduplicated by hash — so a reorg at
//! the tip is re-announced with the replacement block, like a node's own
//! `newHeads`. Ports the legacy `MergedPowHead` / `MergedPosHead`.
//!
//! The announced tip stream is additionally diffed through a [`BlockWindow`]
//! into [`BlockUpdate`]s (`Drop` before `New` on a reorg) — the signal
//! `eth_subscribe("logs")` needs to re-emit dropped logs with `removed: true`
//! (the legacy `ConnectBlockUpdates` diffed the same stream).

use crate::data::{BlockContainer, BlockId};
use crate::upstream::block_updates::{BlockUpdate, BlockWindow};
use crate::upstream::head::CurrentHead;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Capacity of the merged-head broadcast channels. Subscribers only care about
/// the recent blocks, so a small buffer is enough; slow ones simply skip ahead.
const HEAD_CHANNEL_CAPACITY: usize = 16;

/// How many chosen blocks the merge keeps per chain, matching the legacy
/// `MergedHead.headLimit`.
const CHOICE_LIMIT: usize = 16;

/// How competing blocks at the same height are ordered.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum MergeOrder {
    /// PoW: the block with the greater cumulative difficulty wins. Equal
    /// difficulty keeps the incumbent — with absent difficulty (parsed as
    /// zero) everything ties, and replacing on a tie would let any pair of
    /// upstreams flip the head back and forth (the legacy code replaced on
    /// ties; diverges here to keep a misreporting chain stable).
    Difficulty,
    /// PoS: the block from the higher-priority upstream wins; on equal
    /// priority the latest announcement wins.
    Priority,
}

/// A chosen block and the priority of the upstream that reported it (only
/// meaningful for [`MergeOrder::Priority`]).
struct ChoiceEntry {
    priority: i32,
    block: Arc<BlockContainer>,
}

/// The choosing window: legacy `MergedPowHead.onNext` / `MergedPosHead.onNext`.
/// Entries are kept in announcement order, tip last.
struct HeadChoice {
    order: MergeOrder,
    entries: Vec<ChoiceEntry>,
}

impl HeadChoice {
    fn new(order: MergeOrder) -> Self {
        Self {
            order,
            entries: Vec::new(),
        }
    }

    /// Apply one upstream's block; `true` when it changed the chosen chain.
    ///
    /// A repeat of an already-chosen block is accepted, not short-circuited:
    /// it must still be able to trigger the replace-all restart below (the
    /// announce-side hash dedup keeps it from being re-broadcast). Only the
    /// caller's announce dedup makes repeats invisible, same as the legacy
    /// `distinctUntilChanged`.
    fn accept(&mut self, priority: i32, block: Arc<BlockContainer>) -> bool {
        let same_height = self
            .entries
            .iter()
            .position(|e| e.block.height == block.height);
        if let Some(pos) = same_height {
            let prev = &self.entries[pos];
            let loses = match self.order {
                // Strictly greater to displace a *different* block; a repeat of
                // the same block always ties and is ordered out here.
                MergeOrder::Difficulty => block.total_difficulty <= prev.block.total_difficulty,
                MergeOrder::Priority => priority < prev.priority,
            };
            if loses {
                return false;
            }
        }

        let extends_tip = match self.entries.last() {
            None => true,
            Some(tip) => block.height > tip.block.height,
        };
        if extends_tip {
            self.entries.push(ChoiceEntry { priority, block });
            if self.entries.len() > CHOICE_LIMIT {
                self.entries.remove(0);
            }
            return true;
        }
        // A below-tip block from a higher-priority upstream that just came up:
        // restart the window from its view rather than discarding it as stale
        // (legacy `MergedPosHead`'s replace-all branch, checked before the
        // same-height swap just as in the legacy code). This must also fire
        // for a block the window already knows — that's how a recovered
        // primary pulls the head back off a lower-priority upstream's fork.
        if self.order == MergeOrder::Priority && self.entries.iter().all(|e| e.priority < priority)
        {
            self.entries = vec![ChoiceEntry { priority, block }];
            return true;
        }
        if let Some(pos) = same_height {
            self.entries[pos] = ChoiceEntry { priority, block };
            self.prune(pos);
            return true;
        }
        false
    }

    /// After a same-height replacement, blocks above it that are lighter than
    /// the replacement sit on the losing branch — drop them so the tip moves
    /// back to the recognized chain (legacy `MergedPowHead`'s prune).
    fn prune(&mut self, replaced_pos: usize) {
        if self.order != MergeOrder::Difficulty {
            return;
        }
        let height = self.entries[replaced_pos].block.height;
        let difficulty = self.entries[replaced_pos].block.total_difficulty;
        self.entries
            .retain(|e| !(e.block.height > height && e.block.total_difficulty < difficulty));
    }

    fn tip(&self) -> Option<Arc<BlockContainer>> {
        self.entries.last().map(|e| Arc::clone(&e.block))
    }
}

/// Everything the merge mutates, under one lock so concurrent upstream
/// followers can't reorder decisions and broadcasts.
struct MergedState {
    choice: HeadChoice,
    diff: BlockWindow,
    announced: Option<BlockId>,
}

/// A merged view of all upstream heads for one chain.
pub struct MergedHead {
    sender: broadcast::Sender<Arc<BlockContainer>>,
    updates: broadcast::Sender<BlockUpdate>,
    state: Mutex<MergedState>,
}

impl MergedHead {
    /// An empty merge with no subscribers; wire upstreams in with
    /// [`follow`](Self::follow) (or push blocks directly via
    /// [`feed`](Self::feed)).
    pub fn new(order: MergeOrder) -> Self {
        let (sender, _) = broadcast::channel(HEAD_CHANNEL_CAPACITY);
        let (updates, _) = broadcast::channel(HEAD_CHANNEL_CAPACITY);
        Self {
            sender,
            updates,
            state: Mutex::new(MergedState {
                choice: HeadChoice::new(order),
                diff: BlockWindow::new(),
                announced: None,
            }),
        }
    }

    /// Follow one upstream's head for the lifetime of the process, feeding its
    /// blocks into the merge with the upstream's configured priority.
    pub fn follow(self: &Arc<Self>, priority: i32, head: Arc<CurrentHead>) {
        let merged = Arc::clone(self);
        let mut rx = head.subscribe_blocks();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(block) => merged.feed(priority, block),
                    // The head stream keeps only the latest blocks; missing a
                    // few is fine since only the current tip matters.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    /// Apply one upstream's block. When it changes the chosen chain and the
    /// tip's hash actually changed, the tip is re-announced on the head
    /// channel and diffed into `Drop`/`New` updates.
    pub fn feed(&self, priority: i32, block: Arc<BlockContainer>) {
        let mut state = self.state.lock().expect("merged head lock poisoned");
        if !state.choice.accept(priority, block) {
            return;
        }
        let Some(tip) = state.choice.tip() else {
            return;
        };
        // Legacy's `distinctUntilChanged { it.hash }`: a change below the tip
        // is only bookkeeping for later comparisons, nothing is re-announced.
        if state.announced == Some(tip.hash) {
            return;
        }
        state.announced = Some(tip.hash);
        // Send errors just mean no active subscribers.
        for update in state.diff.apply(Arc::clone(&tip)) {
            let _ = self.updates.send(update);
        }
        let _ = self.sender.send(tip);
    }

    /// Subscribe to announced head blocks.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<BlockContainer>> {
        self.sender.subscribe()
    }

    /// Subscribe to canonical-chain updates, including reorg drops.
    pub fn subscribe_updates(&self) -> broadcast::Receiver<BlockUpdate> {
        self.updates.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::BlockId;
    use alloy::primitives::U256;

    fn block(height: u64, hash_byte: u8, difficulty: u64) -> Arc<BlockContainer> {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
        Arc::new(BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: None,
            total_difficulty: U256::from(difficulty),
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        })
    }

    fn recv_now(rx: &mut broadcast::Receiver<Arc<BlockContainer>>) -> Option<Arc<BlockContainer>> {
        rx.try_recv().ok()
    }

    #[tokio::test]
    async fn forwards_advancing_blocks() {
        let merged = MergedHead::new(MergeOrder::Priority);
        let mut rx = merged.subscribe();

        merged.feed(0, block(10, 10, 0));
        merged.feed(0, block(11, 11, 0));

        assert_eq!(rx.recv().await.unwrap().height, 10);
        assert_eq!(rx.recv().await.unwrap().height, 11);
    }

    #[tokio::test]
    async fn ignores_old_and_duplicate_blocks() {
        let merged = MergedHead::new(MergeOrder::Priority);
        let mut rx = merged.subscribe();

        merged.feed(0, block(100, 1, 0));
        // Duplicate hash from another upstream, and a stale lower block.
        merged.feed(1, block(100, 1, 0));
        merged.feed(0, block(98, 2, 0));
        merged.feed(0, block(101, 3, 0));

        assert_eq!(rx.recv().await.unwrap().height, 100);
        assert_eq!(rx.recv().await.unwrap().height, 101);
        assert!(recv_now(&mut rx).is_none());
    }

    #[tokio::test]
    async fn pow_heavier_tip_replaces_and_reannounces() {
        let merged = MergedHead::new(MergeOrder::Difficulty);
        let mut heads = merged.subscribe();
        let mut updates = merged.subscribe_updates();

        let original = block(100, 1, 50);
        let heavier = block(100, 2, 60);
        merged.feed(0, Arc::clone(&original));
        merged.feed(0, Arc::clone(&heavier));

        assert_eq!(heads.recv().await.unwrap().hash, original.hash);
        assert_eq!(heads.recv().await.unwrap().hash, heavier.hash);

        // The updates channel retracts the original before the replacement.
        assert!(
            matches!(updates.recv().await.unwrap(), BlockUpdate::New(b) if b.hash == original.hash)
        );
        assert!(
            matches!(updates.recv().await.unwrap(), BlockUpdate::Drop(b) if b.hash == original.hash)
        );
        assert!(
            matches!(updates.recv().await.unwrap(), BlockUpdate::New(b) if b.hash == heavier.hash)
        );
    }

    #[tokio::test]
    async fn pow_lighter_and_equal_tips_are_ignored() {
        let merged = MergedHead::new(MergeOrder::Difficulty);
        let mut heads = merged.subscribe();

        merged.feed(0, block(100, 1, 50));
        // A competing lighter tip, and a zero-vs-zero style tie: neither wins.
        merged.feed(0, block(100, 2, 40));
        merged.feed(0, block(100, 3, 50));

        assert_eq!(heads.recv().await.unwrap().hash, block(100, 1, 50).hash);
        assert!(recv_now(&mut heads).is_none());
    }

    #[tokio::test]
    async fn pow_reorg_prunes_lighter_descendants() {
        let merged = MergedHead::new(MergeOrder::Difficulty);
        let mut heads = merged.subscribe();

        merged.feed(0, block(100, 1, 50));
        merged.feed(0, block(101, 2, 55));
        // A heavier replacement at 100 invalidates the lighter 101: the tip
        // moves back to the replacement until the new chain re-announces 101.
        let replacement = block(100, 3, 60);
        merged.feed(0, Arc::clone(&replacement));

        assert_eq!(heads.recv().await.unwrap().height, 100);
        assert_eq!(heads.recv().await.unwrap().height, 101);
        assert_eq!(heads.recv().await.unwrap().hash, replacement.hash);
    }

    #[tokio::test]
    async fn pos_higher_priority_wins_and_equal_priority_takes_latest() {
        let merged = MergedHead::new(MergeOrder::Priority);
        let mut heads = merged.subscribe();

        let from_high = block(100, 1, 0);
        let from_low = block(100, 2, 0);
        let from_peer = block(100, 3, 0);

        merged.feed(50, Arc::clone(&from_high));
        // A lower-priority upstream can't displace it…
        merged.feed(10, Arc::clone(&from_low));
        // …but an equal-priority one can: the latest announcement wins.
        merged.feed(50, Arc::clone(&from_peer));

        assert_eq!(heads.recv().await.unwrap().hash, from_high.hash);
        assert_eq!(heads.recv().await.unwrap().hash, from_peer.hash);
        assert!(recv_now(&mut heads).is_none());
    }

    #[tokio::test]
    async fn pos_high_priority_restarts_a_low_priority_window() {
        let merged = MergedHead::new(MergeOrder::Priority);
        let mut heads = merged.subscribe();

        // The chain was followed only through a low-priority upstream.
        merged.feed(10, block(100, 1, 0));
        merged.feed(10, block(101, 2, 0));
        // The high-priority upstream comes up behind the tip: its view
        // restarts the window rather than being discarded as stale.
        let restart = block(100, 3, 0);
        merged.feed(50, Arc::clone(&restart));

        assert_eq!(heads.recv().await.unwrap().height, 100);
        assert_eq!(heads.recv().await.unwrap().height, 101);
        assert_eq!(heads.recv().await.unwrap().hash, restart.hash);
    }

    #[tokio::test]
    async fn pos_recovered_primary_restarts_window_with_a_known_block() {
        let merged = MergedHead::new(MergeOrder::Priority);
        let mut heads = merged.subscribe();

        // A low-priority upstream fed the window, including a forked tip.
        let shared = block(100, 1, 0);
        let forked_tip = block(101, 2, 0);
        merged.feed(10, Arc::clone(&shared));
        merged.feed(10, Arc::clone(&forked_tip));

        // The primary comes back re-announcing a block the window already
        // holds: its view must still restart the window so the served head
        // regresses off the fork, not stay pinned to the forked tip.
        merged.feed(50, Arc::clone(&shared));

        assert_eq!(heads.recv().await.unwrap().hash, shared.hash);
        assert_eq!(heads.recv().await.unwrap().hash, forked_tip.hash);
        assert_eq!(heads.recv().await.unwrap().hash, shared.hash);
    }

    #[tokio::test]
    async fn mid_window_replacement_announces_nothing() {
        let merged = MergedHead::new(MergeOrder::Difficulty);
        let mut heads = merged.subscribe();
        let mut updates = merged.subscribe_updates();

        merged.feed(0, block(100, 1, 50));
        merged.feed(0, block(101, 2, 60));
        // A heavier block at 100 that does NOT invalidate the heavier 101:
        // the tip is unchanged, so nothing is re-announced (legacy
        // `distinctUntilChanged` behavior).
        merged.feed(0, block(100, 3, 55));

        assert_eq!(heads.recv().await.unwrap().height, 100);
        assert_eq!(heads.recv().await.unwrap().height, 101);
        assert!(recv_now(&mut heads).is_none());

        assert!(matches!(updates.recv().await.unwrap(), BlockUpdate::New(_)));
        assert!(matches!(updates.recv().await.unwrap(), BlockUpdate::New(_)));
        assert!(updates.try_recv().is_err());
    }
}
