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
    fn compare_history(
        &self,
        recognized: &[BlockId],
        current: &[BlockId],
        current_height: u64,
    ) -> ForkStatus {
        let Some(recognized_head) = recognized.last().copied() else {
            return ForkStatus::New;
        };
        let Some(current_head) = current.last().copied() else {
            tracing::warn!("Upstream block history is empty");
            return ForkStatus::Rejected { common: 0 };
        };
        if recognized_head == current_head {
            return ForkStatus::Equal;
        }
        if current.iter().any(|b| *b == recognized_head) {
            // current has built on top of the recognized head
            return ForkStatus::Outrun;
        }
        if recognized.iter().any(|b| *b == current_head) {
            // recognized is ahead of current on the same chain
            return ForkStatus::Fallbehind;
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
        ForkStatus::Rejected { common }
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
            Some(ForkStatus::New)
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
        let status = match self.preferential_upstream(upstream_id) {
            None => ForkStatus::New,
            Some(preferred) => {
                let recognized = self.get_blocks(&preferred);
                self.compare_history(&recognized, &head, block.height)
            }
        };
        Some(status)
    }

    fn name(&self) -> &'static str {
        "Priority"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::fork::ForkState;
    use crate::upstream::id::test_id;

    fn id(n: u8) -> BlockId {
        BlockId::from_bytes([n; 32])
    }

    fn block(hash: u8, parent: u8, height: u64) -> BlockContainer {
        BlockContainer {
            hash: id(hash),
            height,
            parent_hash: Some(id(parent)),
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        }
    }

    /// Registers `id` at `priority` with a healthy state so it can act as the
    /// reference chain.
    fn register(fc: &PriorityForkChoice, id: &str, priority: i32) -> Arc<UpstreamState> {
        let state = Arc::new(UpstreamState::new());
        state.update(0, Some(100)); // Ok
        fc.add_upstream(id.parse().unwrap(), priority, Arc::clone(&state));
        state
    }

    #[test]
    fn top_priority_upstream_is_always_new() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        // No higher-priority upstream exists, so it's trusted.
        assert_eq!(
            fc.submit(&block(2, 1, 1), &test_id("top")),
            Some(ForkStatus::New)
        );
    }

    #[test]
    fn lower_priority_on_same_block_is_equal() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        register(&fc, "low", 50);
        fc.submit(&block(2, 1, 1), &test_id("top"));
        assert_eq!(
            fc.submit(&block(2, 1, 1), &test_id("low")),
            Some(ForkStatus::Equal)
        );
    }

    #[test]
    fn lower_priority_behind_is_fallbehind() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        register(&fc, "low", 50);
        // top advances 1 -> 2 -> 3, low is still at 2
        fc.submit(&block(2, 1, 1), &test_id("top"));
        fc.submit(&block(3, 2, 2), &test_id("top"));
        assert_eq!(
            fc.submit(&block(2, 1, 1), &test_id("low")),
            Some(ForkStatus::Fallbehind)
        );
    }

    #[test]
    fn lower_priority_ahead_is_outrun() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        register(&fc, "low", 50);
        fc.submit(&block(2, 1, 1), &test_id("top"));
        // low builds on top's head (2 -> 3) before top sees block 3
        assert_eq!(
            fc.submit(&block(3, 2, 2), &test_id("low")),
            Some(ForkStatus::Outrun)
        );
    }

    #[test]
    fn divergent_chain_is_rejected() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        register(&fc, "low", 50);
        // top: 1 -> 2 -> 3
        fc.submit(&block(2, 1, 1), &test_id("top"));
        fc.submit(&block(3, 2, 2), &test_id("top"));
        // low forks off a different parent: 10 -> 11, unrelated to top's chain
        fc.submit(&block(10, 9, 1), &test_id("low"));
        assert_eq!(
            fc.submit(&block(11, 10, 2), &test_id("low")),
            Some(ForkStatus::Rejected { common: 0 })
        );
    }

    #[test]
    fn rejected_reports_the_last_shared_height() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        register(&fc, "low", 50);
        // top: 2 -> 3 -> 4, at heights 1..=3
        fc.submit(&block(2, 1, 1), &test_id("top"));
        fc.submit(&block(3, 2, 2), &test_id("top"));
        fc.submit(&block(4, 3, 3), &test_id("top"));
        // low shares block 2 (height 1), then goes its own way: 2 -> 20 -> 21
        fc.submit(&block(2, 1, 1), &test_id("low"));
        fc.submit(&block(20, 2, 2), &test_id("low"));
        assert_eq!(
            fc.submit(&block(21, 20, 3), &test_id("low")),
            Some(ForkStatus::Rejected { common: 1 })
        );
    }

    fn unlinked(hash: u8, height: u64) -> BlockContainer {
        let mut b = block(hash, 0, height);
        b.parent_hash = None;
        b
    }

    #[test]
    fn top_priority_without_parent_is_new() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        assert_eq!(
            fc.submit(&unlinked(2, 1), &test_id("top")),
            Some(ForkStatus::New)
        );
    }

    #[test]
    fn lower_priority_without_parent_is_placed_by_hash() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        register(&fc, "low", 50);
        fc.submit(&block(2, 1, 1), &test_id("top"));
        fc.submit(&block(3, 2, 2), &test_id("top"));
        assert_eq!(
            fc.submit(&unlinked(3, 2), &test_id("low")),
            Some(ForkStatus::Equal)
        );
        assert_eq!(
            fc.submit(&unlinked(2, 1), &test_id("low")),
            Some(ForkStatus::Fallbehind)
        );
    }

    #[test]
    fn block_without_parent_off_the_recognized_chain_gets_no_verdict() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        register(&fc, "low", 50);
        // low is correctly judged forked from linked blocks: top 2 -> 3 -> 4,
        // low 2 -> 20 -> 21.
        fc.submit(&block(2, 1, 1), &test_id("top"));
        fc.submit(&block(3, 2, 2), &test_id("top"));
        fc.submit(&block(4, 3, 3), &test_id("top"));
        fc.submit(&block(2, 1, 1), &test_id("low"));
        fc.submit(&block(20, 2, 2), &test_id("low"));
        fc.submit(&block(21, 20, 3), &test_id("low"));
        // A block that can't be placed must not clear that verdict.
        assert_eq!(fc.submit(&unlinked(22, 4), &test_id("low")), None);
    }

    #[test]
    fn block_without_parent_is_new_while_the_reference_has_no_chain() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        register(&fc, "low", 50);
        assert_eq!(
            fc.submit(&unlinked(2, 1), &test_id("low")),
            Some(ForkStatus::New)
        );
    }

    #[test]
    fn reference_without_parents_does_not_leave_a_stale_chain() {
        let fc = PriorityForkChoice::new();
        register(&fc, "top", 100);
        register(&fc, "low", 50);
        fc.submit(&block(2, 1, 1), &test_id("top"));
        fc.submit(&block(3, 2, 2), &test_id("top"));
        // From here top's heads come without parents, so its tracked chain
        // can't follow them.
        fc.submit(&unlinked(4, 3), &test_id("top"));
        // low keeps following the real chain well past the tracking window;
        // it must not end up judged against top's frozen chain.
        let mut status = None;
        for hash in 2..=(TRACK_LIMIT as u8 + 10) {
            status = fc.submit(&block(hash, hash - 1, hash as u64 - 1), &test_id("low"));
        }
        assert_eq!(status, Some(ForkStatus::New));
    }

    #[test]
    fn unhealthy_preferential_is_skipped() {
        let fc = PriorityForkChoice::new();
        let top = register(&fc, "top", 100);
        register(&fc, "low", 50);
        // top is forked/immature, so it must not be used as the reference;
        // low then has no healthy upstream above it and is trusted as New.
        top.set_fork(ForkState::Forked { common: 0 });
        fc.submit(&block(2, 1, 1), &test_id("top"));
        assert_eq!(
            fc.submit(&block(10, 9, 1), &test_id("low")),
            Some(ForkStatus::New)
        );
    }
}
