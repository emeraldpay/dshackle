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

//! The recent canonical chain as announced so far, and how a new branch
//! attaches to it.

use super::HISTORY_LIMIT;
use crate::data::{BlockContainer, BlockId};
use std::collections::VecDeque;
use std::sync::Arc;

/// A block of the continuous head stream.
#[derive(Clone, Debug)]
pub struct SequencedBlock {
    pub block: Arc<BlockContainer>,
    /// How many previously announced blocks were reorganized away right
    /// before this one. Non-zero only on the first block of a new branch, so
    /// a subscriber sees the height step back exactly where the reorg is.
    pub reorg: u64,
}

/// The most recent announced blocks, oldest first, each a child of the one
/// before it (by parent hash when known, by height otherwise).
#[derive(Clone, Default)]
pub struct HeadHistory {
    blocks: VecDeque<Arc<BlockContainer>>,
}

impl HeadHistory {
    pub fn tip(&self) -> Option<&Arc<BlockContainer>> {
        self.blocks.back()
    }

    pub fn blocks(&self) -> impl Iterator<Item = &Arc<BlockContainer>> {
        self.blocks.iter()
    }

    /// The lowest height the history still holds a block for.
    pub fn first_height(&self) -> Option<u64> {
        self.blocks.front().map(|b| b.height)
    }

    /// Whether `tip` is below everything the history holds. Such a tip can't
    /// be attached, and it's far more likely an upstream far behind (a node
    /// resyncing, yet preferred by priority) than a reorg that deep.
    pub fn is_stale(&self, tip: &BlockContainer) -> bool {
        self.first_height()
            .is_some_and(|lowest| tip.height < lowest)
    }

    pub fn contains(&self, hash: &BlockId) -> bool {
        self.position(hash).is_some()
    }

    fn position(&self, hash: &BlockId) -> Option<usize> {
        self.blocks.iter().position(|b| b.hash == *hash)
    }

    /// Make `branch` the new end of the chain and return what to announce.
    ///
    /// `branch` is a run of parent-linked blocks, oldest first, ending at the
    /// new tip. It may overlap what's already announced: the newest block the
    /// history already holds is where it attaches, and only the blocks after
    /// it are new. Every announced block above that point is reorganized away
    /// and counted in the first new block's `reorg`.
    ///
    /// A branch with nothing new — the tip moving back to an already announced
    /// block — changes nothing: the blocks above it stay announced until a new
    /// block shows whether they were really replaced, so a head that briefly
    /// regresses and recovers never looks like a reorg.
    ///
    /// A branch that can't be attached (its ancestry beyond what the history
    /// holds, or couldn't be fetched) is announced as is, best effort: the
    /// announced blocks at its heights or above count as reorganized, and the
    /// history restarts from it. Unless its tip is below the whole history:
    /// that's a [stale](Self::is_stale) head, ignored like a short regression.
    /// A real reorg that deep shows up with the next block of the new chain.
    pub fn extend(&mut self, branch: Vec<Arc<BlockContainer>>) -> Vec<SequencedBlock> {
        let known = branch.iter().rposition(|b| self.contains(&b.hash));
        let (attach_at, fresh) = match known {
            Some(i) => (self.position(&branch[i].hash), &branch[i + 1..]),
            None => (
                branch
                    .first()
                    .and_then(|b| b.parent_hash)
                    .and_then(|parent| self.position(&parent)),
                &branch[..],
            ),
        };
        let Some(first) = fresh.first() else {
            return Vec::new();
        };
        let reorg = match attach_at {
            Some(pos) => self.truncate_after(pos),
            None if fresh.last().is_some_and(|tip| self.is_stale(tip)) => return Vec::new(),
            None => self.detach(first),
        };

        self.blocks.extend(fresh.iter().cloned());
        while self.blocks.len() > HISTORY_LIMIT {
            self.blocks.pop_front();
        }
        fresh
            .iter()
            .enumerate()
            .map(|(i, block)| SequencedBlock {
                block: Arc::clone(block),
                reorg: if i == 0 { reorg } else { 0 },
            })
            .collect()
    }

    /// Drop everything above `pos`, returning how many blocks were dropped.
    fn truncate_after(&mut self, pos: usize) -> u64 {
        let dropped = self.blocks.len() - pos - 1;
        self.blocks.truncate(pos + 1);
        dropped as u64
    }

    /// Make room for `first`, a block that doesn't attach by hash, returning
    /// how many announced blocks it replaces.
    ///
    /// Without a parent hash (a bare head from a remote Dshackle that couldn't
    /// be resolved) the height is the only hint: a block right above the
    /// remaining tip is taken as its child. Otherwise the history no longer
    /// describes the ancestry of what comes next and is cleared.
    fn detach(&mut self, first: &BlockContainer) -> u64 {
        let kept = self
            .blocks
            .iter()
            .take_while(|b| b.height < first.height)
            .count();
        let replaced = self.blocks.len() - kept;
        self.blocks.truncate(kept);
        let follows_by_height = self
            .tip()
            .is_some_and(|tip| first.parent_hash.is_none() && tip.height + 1 == first.height);
        if !follows_by_height {
            self.blocks.clear();
        }
        replaced as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::continuous_head::testing::{Chain, block};

    fn announced(events: &[SequencedBlock]) -> Vec<(u64, u8, u64)> {
        events
            .iter()
            .map(|e| (e.block.height, e.block.hash.as_bytes()[0], e.reorg))
            .collect()
    }

    fn history_of(blocks: &[Arc<BlockContainer>]) -> HeadHistory {
        let mut history = HeadHistory::default();
        history.extend(blocks.to_vec());
        history
    }

    #[test]
    fn first_branch_is_announced_whole() {
        let chain = Chain::new(1).grow(100, 3);
        let mut history = HeadHistory::default();

        let events = history.extend(chain.blocks.clone());

        assert_eq!(
            announced(&events),
            vec![(100, 1, 0), (101, 1, 0), (102, 1, 0)]
        );
    }

    #[test]
    fn child_of_the_tip_is_announced() {
        let chain = Chain::new(1).grow(100, 3);
        let mut history = history_of(&chain.blocks[..2]);

        let events = history.extend(vec![Arc::clone(&chain.blocks[2])]);

        assert_eq!(announced(&events), vec![(102, 1, 0)]);
    }

    #[test]
    fn overlapping_branch_announces_only_the_new_part() {
        let chain = Chain::new(1).grow(100, 4);
        let mut history = history_of(&chain.blocks[..2]);

        let events = history.extend(chain.blocks.clone());

        assert_eq!(announced(&events), vec![(102, 1, 0), (103, 1, 0)]);
    }

    #[test]
    fn repeated_tip_announces_nothing() {
        let chain = Chain::new(1).grow(100, 2);
        let mut history = history_of(&chain.blocks);

        assert!(
            history
                .extend(vec![Arc::clone(&chain.blocks[1])])
                .is_empty()
        );
    }

    #[test]
    fn replacement_at_the_tip_is_a_reorg_of_one() {
        let main = Chain::new(1).grow(100, 2);
        let fork = main.fork_at(101, 2).grow(101, 1);
        let mut history = history_of(&main.blocks);

        let events = history.extend(fork.from(101));

        assert_eq!(announced(&events), vec![(101, 2, 1)]);
    }

    #[test]
    fn branch_from_below_the_tip_replaces_everything_above_the_fork() {
        let main = Chain::new(1).grow(100, 4);
        let fork = main.fork_at(101, 2).grow(101, 4);
        let mut history = history_of(&main.blocks);

        let events = history.extend(fork.from(101));

        assert_eq!(
            announced(&events),
            vec![(101, 2, 3), (102, 2, 0), (103, 2, 0), (104, 2, 0)]
        );
        assert_eq!(history.tip().unwrap().height, 104);
    }

    #[test]
    fn regression_to_an_announced_block_waits_for_the_next_block() {
        let main = Chain::new(1).grow(100, 3);
        let mut history = history_of(&main.blocks);

        assert!(history.extend(vec![Arc::clone(&main.blocks[0])]).is_empty());
        // The same chain continuing: nothing was ever replaced.
        let next = main.grow(103, 1);
        let events = history.extend(next.from(103));
        assert_eq!(announced(&events), vec![(103, 1, 0)]);
    }

    #[test]
    fn regression_followed_by_a_fork_reports_the_full_reorg() {
        let main = Chain::new(1).grow(100, 3);
        let fork = main.fork_at(101, 2).grow(101, 1);
        let mut history = history_of(&main.blocks);

        assert!(history.extend(vec![Arc::clone(&main.blocks[0])]).is_empty());
        let events = history.extend(fork.from(101));

        assert_eq!(announced(&events), vec![(101, 2, 2)]);
    }

    #[test]
    fn tip_below_the_history_is_ignored() {
        let chain = Chain::new(1).grow(100, HISTORY_LIMIT as u64);
        let mut history = history_of(&chain.blocks);

        assert!(history.extend(vec![Arc::new(block(15, 2))]).is_empty());
        assert_eq!(history.first_height(), Some(100));

        let next = chain.grow(116, 1);
        let events = history.extend(next.from(116));
        assert_eq!(announced(&events), vec![(116, 1, 0)]);
    }

    #[test]
    fn unattached_tip_at_the_lowest_height_still_replaces_the_history() {
        let main = Chain::new(1).grow(100, 3);
        let fork = Chain::new(2).grow(100, 1);
        let mut history = history_of(&main.blocks);

        let events = history.extend(fork.blocks.clone());

        assert_eq!(announced(&events), vec![(100, 2, 3)]);
    }

    #[test]
    fn unattached_gap_is_announced_and_restarts_the_history() {
        let chain = Chain::new(1).grow(100, 10);
        let mut history = history_of(&chain.blocks[..2]);

        let events = history.extend(chain.from(105));

        assert_eq!(
            announced(&events)
                .iter()
                .map(|(h, _, r)| (*h, *r))
                .collect::<Vec<_>>(),
            vec![(105, 0), (106, 0), (107, 0), (108, 0), (109, 0)]
        );
        assert_eq!(history.first_height(), Some(105));
    }

    #[test]
    fn unattached_fork_counts_the_announced_blocks_it_covers() {
        let main = Chain::new(1).grow(100, 5);
        // Forked below everything the history holds.
        let fork = main.fork_at(99, 2).grow(102, 4);
        let mut history = history_of(&main.blocks);

        let events = history.extend(fork.from(102));

        assert_eq!(announced(&events)[0], (102, 2, 3));
        assert_eq!(history.first_height(), Some(102));
    }

    #[test]
    fn parentless_block_right_above_the_tip_is_taken_as_its_child() {
        let chain = Chain::new(1).grow(100, 2);
        let mut history = history_of(&chain.blocks);
        let mut bare = block(102, 3);
        bare.parent_hash = None;

        let events = history.extend(vec![Arc::new(bare)]);

        assert_eq!(announced(&events), vec![(102, 3, 0)]);
        assert_eq!(history.first_height(), Some(100));
    }

    #[test]
    fn parentless_block_at_an_announced_height_replaces_it() {
        let chain = Chain::new(1).grow(100, 3);
        let mut history = history_of(&chain.blocks);
        let mut bare = block(102, 3);
        bare.parent_hash = None;

        let events = history.extend(vec![Arc::new(bare)]);

        assert_eq!(announced(&events), vec![(102, 3, 1)]);
        assert_eq!(history.first_height(), Some(100));
    }

    #[test]
    fn history_keeps_only_the_limit() {
        let chain = Chain::new(1).grow(100, HISTORY_LIMIT as u64 + 5);
        let mut history = HeadHistory::default();

        history.extend(chain.blocks.clone());

        assert_eq!(history.blocks().count(), HISTORY_LIMIT);
        assert_eq!(history.first_height(), Some(105));
    }
}
