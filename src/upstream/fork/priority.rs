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

//! Priority-based fork choice for Proof-of-Stake chains.

use super::{ForkChoice, ForkStatus};
use crate::data::{BlockContainer, BlockId};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::id::UpstreamId;
use crate::upstream::state::UpstreamState;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

/// How many blocks of an upstream's chain to keep. A healthy high-priority
/// upstream is always preferred and any lagging one is treated as unhealthy, so
/// a short window suffices.
const TRACK_LIMIT: usize = 24;
/// Parent→child relations kept to fill gaps when rebuilding a chain.
const JOURNAL_LIMIT: usize = TRACK_LIMIT * 2;

/// A registered upstream and the bits the fork choice needs from it.
struct Member {
    id: UpstreamId,
    priority: i32,
    state: Arc<UpstreamState>,
}

/// Detects forks by upstream priority: an upstream is forked when its head sits
/// on a chain that diverges from the highest-priority *healthy* upstream's
/// chain. The top-priority upstream is always trusted. Ported from the legacy
/// `PriorityForkChoice`.
pub struct PriorityForkChoice {
    /// Registered upstreams, ordered by registration. Read on every submit.
    members: RwLock<Vec<Member>>,
    /// Recent parent→child relations, oldest at the front.
    journal: Mutex<VecDeque<(BlockId, BlockId)>>,
    /// The reconstructed recent chain for each upstream, oldest first.
    blocks: Mutex<HashMap<UpstreamId, Vec<BlockId>>>,
    /// Upstreams already reported for sending blocks without a parent hash,
    /// so the warning shows once rather than on every block.
    reported_unlinked: Mutex<HashSet<UpstreamId>>,
}

impl PriorityForkChoice {
    pub fn new() -> Self {
        Self {
            members: RwLock::new(Vec::new()),
            journal: Mutex::new(VecDeque::new()),
            blocks: Mutex::new(HashMap::new()),
            reported_unlinked: Mutex::new(HashSet::new()),
        }
    }

    /// Register an upstream with its priority. All upstreams are known at
    /// startup, so this replaces the legacy reactive registration.
    pub fn add_upstream(&self, id: UpstreamId, priority: i32, state: Arc<UpstreamState>) {
        let mut members = self.members.write().unwrap();
        if members.iter().any(|m| m.priority == priority && m.id != id) {
            tracing::warn!(
                priority,
                "Two upstreams share the same priority; fork choice may behave unpredictably"
            );
        }
        members.push(Member {
            id,
            priority,
            state,
        });
    }

    /// The best upstream ranked above `current_id` that is currently healthy,
    /// i.e. the one whose chain `current` should agree with. `None` when
    /// `current` is itself the top-priority healthy upstream.
    fn preferential_upstream(&self, current_id: &UpstreamId) -> Option<UpstreamId> {
        let members = self.members.read().unwrap();
        let current_priority = members.iter().find(|m| &m.id == current_id)?.priority;
        members
            .iter()
            .filter(|m| m.priority > current_priority)
            // Trust only healthy upstreams (better than Immature) — a forked or
            // syncing upstream must not become the reference chain.
            .filter(|m| m.state.availability() < UpstreamAvailability::Immature)
            .max_by_key(|m| m.priority)
            .map(|m| m.id.clone())
    }

    fn get_blocks(&self, id: &UpstreamId) -> Vec<BlockId> {
        self.blocks
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .unwrap_or_default()
    }

    fn set_blocks(&self, id: &UpstreamId, chain: Vec<BlockId>) {
        self.blocks.lock().unwrap().insert(id.clone(), chain);
    }

    /// Remember a parent→child relation for later gap-filling. Returns whether
    /// it was new.
    fn keep_journal(&self, parent: BlockId, current: BlockId) -> bool {
        let mut journal = self.journal.lock().unwrap();
        let exists = journal
            .iter()
            .rev()
            .any(|(p, c)| *p == parent && *c == current);
        if !exists {
            journal.push_back((parent, current));
            if journal.len() > JOURNAL_LIMIT {
                journal.pop_front();
            }
        }
        !exists
    }

    /// Find the recorded parent relation whose child is `block`.
    fn find_parent(&self, block: BlockId) -> Option<(BlockId, BlockId)> {
        self.journal
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(_, c)| *c == block)
            .copied()
    }

    /// Rebuild a chain from scratch, walking parents through the journal to
    /// connect `current` back to the `existing` chain where possible.
    fn rebuild(&self, existing: &[BlockId], parent: BlockId, current: BlockId) -> Vec<BlockId> {
        let mut result: VecDeque<BlockId> = VecDeque::new();
        result.push_back(parent);
        result.push_back(current);
        while (existing.is_empty() || existing.last() != result.front())
            && result.len() < TRACK_LIMIT
        {
            let front = *result.front().expect("result is never empty");
            match self.find_parent(front) {
                Some((found_parent, _)) => result.push_front(found_parent),
                None => break,
            }
        }
        if !existing.is_empty() && existing.last() == result.front() {
            // The rebuilt chain reconnects to the known one — splice them,
            // dropping the shared block.
            let mut merged = existing.to_vec();
            merged.extend(result.iter().skip(1).copied());
            return merged;
        }
        result.into_iter().collect()
    }

    /// Append `current` to the upstream's `existing` chain, forking from the
    /// known position of `parent` if it diverges. Rebuilds when the parent is
    /// unknown.
    fn merge(&self, existing: &[BlockId], parent: BlockId, current: BlockId) -> Vec<BlockId> {
        if existing.is_empty() {
            return self.rebuild(existing, parent, current);
        }
        let Some(parent_pos) = existing.iter().position(|b| *b == parent) else {
            return self.rebuild(existing, parent, current);
        };
        let start = parent_pos.saturating_sub(TRACK_LIMIT);
        let mut result = existing[start..=parent_pos].to_vec();
        result.push(current);
        result
    }

    /// Compare an upstream's `current` chain, whose last block is at
    /// `current_height`, against the `recognized` chain of the preferential
    /// upstream.
    ///
    /// `None` while the recognized chain is empty: at startup, or after the
    /// preferential upstream's head moved without a link (see
    /// [`forget_stale_chain`](Self::forget_stale_chain)). There is nothing to
    /// compare with then, and calling the upstream `New` would clear a fork
    /// it's still on.
    fn compare_history(
        &self,
        recognized: &[BlockId],
        current: &[BlockId],
        current_height: u64,
    ) -> Option<ForkStatus> {
        let recognized_head = recognized.last().copied()?;
        let Some(current_head) = current.last().copied() else {
            tracing::warn!("Upstream block history is empty");
            return Some(ForkStatus::Rejected { common: 0 });
        };
        if recognized_head == current_head {
            return Some(ForkStatus::Equal);
        }
        if current.iter().any(|b| *b == recognized_head) {
            // current has built on top of the recognized head
            return Some(ForkStatus::Outrun);
        }
        if recognized.iter().any(|b| *b == current_head) {
            // recognized is ahead of current on the same chain
            return Some(ForkStatus::Fallbehind);
        }
        // A chain is kept contiguous, parent to child, so a block's height is
        // its distance from the last one. With no tracked block in common the
        // fork point is below both windows, and genesis is the only block
        // known to be shared.
        let common = current
            .iter()
            .rposition(|b| recognized.contains(b))
            .map_or(0, |pos| {
                current_height.saturating_sub((current.len() - 1 - pos) as u64)
            });
        Some(ForkStatus::Rejected { common })
    }

    /// Judge a block that came without its parent hash (e.g. a Dshackle
    /// upstream whose head couldn't be fetched in full). It can't be linked
    /// into a chain, so only its own hash is checked against the recognized
    /// chain, and a block that can't be placed there gets no verdict at all:
    /// legacy flagged it as a fork, which knocks a healthy upstream out of
    /// routing over a missing field, while calling it on-chain would clear a
    /// real fork.
    fn judge_unlinked(
        &self,
        block: &BlockContainer,
        upstream_id: &UpstreamId,
    ) -> Option<ForkStatus> {
        if self
            .reported_unlinked
            .lock()
            .unwrap()
            .insert(upstream_id.clone())
        {
            tracing::warn!(
                upstream = %upstream_id,
                "Upstream sends blocks without a parent hash; fork detection can't follow its chain"
            );
        }
        self.forget_stale_chain(upstream_id, block.hash);
        let Some(preferred) = self.preferential_upstream(upstream_id) else {
            return Some(ForkStatus::New);
        };
        let recognized = self.get_blocks(&preferred);
        if recognized.is_empty() {
            // Same as for a linked block: nothing to compare with.
            None
        } else if recognized.last() == Some(&block.hash) {
            Some(ForkStatus::Equal)
        } else if recognized.contains(&block.hash) {
            Some(ForkStatus::Fallbehind)
        } else {
            tracing::debug!(upstream = %upstream_id, hash = %block.hash, "Block without parent hash can't be placed on the recognized chain");
            None
        }
    }

    /// Drop the upstream's tracked chain once its head has moved to `head`
    /// without a link to it. The chain no longer describes where the
    /// upstream stands, and when it's the reference for the others they'd
    /// keep comparing against it until their own windows slide past it and
    /// they look forked.
    fn forget_stale_chain(&self, upstream_id: &UpstreamId, head: BlockId) {
        let mut blocks = self.blocks.lock().unwrap();
        if blocks.get(upstream_id).and_then(|chain| chain.last()) != Some(&head) {
            blocks.remove(upstream_id);
        }
    }
}

impl ForkChoice for PriorityForkChoice {
    fn submit(&self, block: &BlockContainer, upstream_id: &UpstreamId) -> Option<ForkStatus> {
        let Some(parent) = block.parent_hash else {
            return self.judge_unlinked(block, upstream_id);
        };
        self.keep_journal(parent, block.hash);
        let history = self.get_blocks(upstream_id);
        let head = self.merge(&history, parent, block.hash);
        self.set_blocks(upstream_id, head.clone());
        match self.preferential_upstream(upstream_id) {
            None => Some(ForkStatus::New),
            Some(preferred) => {
                let recognized = self.get_blocks(&preferred);
                self.compare_history(&recognized, &head, block.height)
            }
        }
    }

    fn name(&self) -> &'static str {
        "Priority"
    }
}

#[cfg(test)]
mod tests {
    //! The specification, told from the side of `low`, a lower-priority
    //! upstream judged against `top`, the reference that sets the recognized
    //! chain. Blocks are named by where they sit: [`recognized`] for the
    //! recognized chain, [`fork`] for a chain that left it, and
    //! [`without_parent`] for a block that arrives with its parent unknown.

    use super::*;
    use crate::upstream::fork::ForkState;
    use crate::upstream::id::test_id;

    const RECOGNIZED: u64 = 0;

    /// Distinct per branch and height, so each block is spelled out by where
    /// it sits rather than by an arbitrary number.
    fn block_id(branch: u64, height: u64) -> BlockId {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&branch.to_be_bytes());
        bytes[24..].copy_from_slice(&height.to_be_bytes());
        BlockId::from_bytes(bytes)
    }

    fn linked(hash: BlockId, parent: BlockId, height: u64) -> BlockContainer {
        BlockContainer {
            hash,
            height,
            parent_hash: Some(parent),
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        }
    }

    /// The block at `height` on the recognized chain.
    fn recognized(height: u64) -> BlockContainer {
        linked(
            block_id(RECOGNIZED, height),
            block_id(RECOGNIZED, height - 1),
            height,
        )
    }

    /// The block at `height` on a chain that left the recognized one after
    /// its block at `from`.
    fn fork(from: u64, height: u64) -> BlockContainer {
        assert!(height > from, "a fork starts above its fork point");
        let branch = from + 1;
        let parent = if height == from + 1 {
            block_id(RECOGNIZED, from)
        } else {
            block_id(branch, height - 1)
        };
        linked(block_id(branch, height), parent, height)
    }

    /// `block` as a Dshackle upstream reports it when the full block couldn't
    /// be fetched.
    fn without_parent(mut block: BlockContainer) -> BlockContainer {
        block.parent_hash = None;
        block
    }

    /// The fork choice of a chain with `top` (priority 100) and `low` (50).
    struct Upstreams {
        fork_choice: PriorityForkChoice,
        top_state: Arc<UpstreamState>,
    }

    impl Upstreams {
        fn new() -> Self {
            let fork_choice = PriorityForkChoice::new();
            let top_state = Self::register(&fork_choice, "top", 100);
            Self::register(&fork_choice, "low", 50);
            Self {
                fork_choice,
                top_state,
            }
        }

        /// Healthy (no lag, past block 0), so it can act as the reference.
        fn register(fc: &PriorityForkChoice, id: &str, priority: i32) -> Arc<UpstreamState> {
            let state = Arc::new(UpstreamState::new());
            state.update(0, Some(100));
            fc.add_upstream(test_id(id), priority, Arc::clone(&state));
            state
        }

        fn top(&self, block: BlockContainer) -> Option<ForkStatus> {
            self.fork_choice.submit(&block, &test_id("top"))
        }

        fn low(&self, block: BlockContainer) -> Option<ForkStatus> {
            self.fork_choice.submit(&block, &test_id("low"))
        }

        fn top_follows(&self, blocks: impl IntoIterator<Item = BlockContainer>) {
            for block in blocks {
                self.top(block);
            }
        }

        /// `low` reaches its fork verdict from linked blocks: it shares the
        /// recognized chain up to height 1 and then goes its own way, while
        /// `top` is at height 3.
        fn with_low_forked(self) -> Self {
            self.top_follows((1..=3).map(recognized));
            self.low(recognized(1));
            self.low(fork(1, 2));
            assert_eq!(
                self.low(fork(1, 3)),
                Some(ForkStatus::Rejected { common: 1 })
            );
            self
        }
    }

    /// Blocks that carry their parent, so each upstream's chain is linked
    /// and compared block by block.
    mod linked_blocks {
        use super::*;

        #[test]
        fn top_is_always_new() {
            let upstreams = Upstreams::new();
            assert_eq!(upstreams.top(recognized(1)), Some(ForkStatus::New));
            // Nothing ranks above top, so even a fork of its own is trusted.
            assert_eq!(upstreams.top(fork(1, 2)), Some(ForkStatus::New));
        }

        #[test]
        fn low_at_the_recognized_head_is_equal() {
            let upstreams = Upstreams::new();
            upstreams.top(recognized(1));
            assert_eq!(upstreams.low(recognized(1)), Some(ForkStatus::Equal));
        }

        #[test]
        fn low_behind_on_the_recognized_chain_is_fallbehind() {
            let upstreams = Upstreams::new();
            upstreams.top_follows((1..=2).map(recognized));
            assert_eq!(upstreams.low(recognized(1)), Some(ForkStatus::Fallbehind));
        }

        #[test]
        fn low_ahead_on_the_recognized_chain_is_outrun() {
            let upstreams = Upstreams::new();
            upstreams.top(recognized(1));
            assert_eq!(upstreams.low(recognized(2)), Some(ForkStatus::Outrun));
        }

        #[test]
        fn low_on_a_fork_is_rejected_at_the_last_shared_height() {
            // The setup asserts the verdict itself.
            Upstreams::new().with_low_forked();
        }

        #[test]
        fn fork_point_below_the_tracked_blocks_is_reported_as_genesis() {
            let upstreams = Upstreams::new();
            upstreams.top_follows((30..=32).map(recognized));
            // Forked at height 10, long before either chain's tracked blocks
            // begin: the only block known to be shared is genesis.
            upstreams.low(fork(10, 31));
            assert_eq!(
                upstreams.low(fork(10, 32)),
                Some(ForkStatus::Rejected { common: 0 })
            );
        }
    }

    /// Which upstream sets the recognized chain, and what happens when none
    /// can.
    mod reference {
        use super::*;

        #[test]
        fn unhealthy_top_is_not_the_reference() {
            let upstreams = Upstreams::new();
            upstreams
                .top_state
                .set_fork(ForkState::Forked { common: 0 });
            upstreams.top(recognized(1));
            // No healthy upstream ranks above low, so it's trusted itself.
            assert_eq!(upstreams.low(fork(0, 1)), Some(ForkStatus::New));
        }

        #[test]
        fn no_verdict_before_the_reference_reports_a_block() {
            let upstreams = Upstreams::new();
            assert_eq!(upstreams.low(recognized(1)), None);
            assert_eq!(upstreams.low(without_parent(recognized(2))), None);
        }
    }

    /// Blocks whose parent is unknown can't be linked into a chain; only
    /// their own hash can be checked.
    mod blocks_without_parent {
        use super::*;

        #[test]
        fn top_is_always_new() {
            let upstreams = Upstreams::new();
            assert_eq!(
                upstreams.top(without_parent(recognized(1))),
                Some(ForkStatus::New)
            );
        }

        #[test]
        fn low_found_on_the_recognized_chain_is_placed_by_its_hash() {
            let upstreams = Upstreams::new();
            upstreams.top_follows((1..=2).map(recognized));
            assert_eq!(
                upstreams.low(without_parent(recognized(2))),
                Some(ForkStatus::Equal)
            );
            assert_eq!(
                upstreams.low(without_parent(recognized(1))),
                Some(ForkStatus::Fallbehind)
            );
        }

        #[test]
        fn low_not_found_on_the_recognized_chain_keeps_its_verdict() {
            let upstreams = Upstreams::new().with_low_forked();
            // Calling it on-chain would clear the fork; calling it forked
            // would flag a healthy upstream that merely lacks the field.
            assert_eq!(upstreams.low(without_parent(fork(1, 4))), None);
        }

        #[test]
        fn reference_losing_its_chain_keeps_low_forked() {
            let upstreams = Upstreams::new().with_low_forked();
            // top's head moves without a link, so its tracked chain is gone.
            upstreams.top(without_parent(recognized(4)));
            assert_eq!(upstreams.low(fork(1, 4)), None);
        }

        #[test]
        fn reference_repeating_its_head_keeps_its_chain() {
            let upstreams = Upstreams::new();
            upstreams.top_follows((1..=3).map(recognized));
            // The same head again, e.g. re-announced after a reconnect: its
            // chain still describes where top stands.
            upstreams.top(without_parent(recognized(3)));
            assert_eq!(upstreams.low(recognized(2)), Some(ForkStatus::Fallbehind));
        }

        #[test]
        fn reference_without_parents_leaves_no_stale_chain_behind() {
            let upstreams = Upstreams::new();
            upstreams.top_follows((1..=2).map(recognized));
            upstreams.top(without_parent(recognized(3)));
            // low keeps following the recognized chain well past the tracked
            // window; judged against top's chain as of height 2, it would
            // look forked once that falls out of its window.
            for height in 1..=(TRACK_LIMIT as u64 + 10) {
                let status = upstreams.low(recognized(height));
                assert!(
                    !matches!(status, Some(ForkStatus::Rejected { .. })),
                    "low judged forked at height {height}"
                );
            }
        }
    }
}
