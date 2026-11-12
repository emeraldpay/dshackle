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

use super::{
    DifficultyForkChoice, ForkChoice, ForkMember, ForkStatus, NeverForkChoice, PriorityForkChoice,
    is_pos, start_fork_watch,
};
use crate::blockchain::TargetBlockchain;
use crate::data::BlockContainer;
use crate::upstream::id::UpstreamId;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The fork detection of one chain's upstreams, which may join it at any time
/// (a remote Dshackle connected after the start).
pub struct ForkGroup {
    chain: TargetBlockchain,
    /// The chain's actual fork choice: by priority for a PoS chain, where
    /// difficulty no longer grows, and by difficulty otherwise.
    choice: Arc<dyn ForkChoice>,
    /// The same choice, to register the members with when it's by priority.
    by_priority: Option<Arc<PriorityForkChoice>>,
    /// The verdict while the chain has a single upstream: with nothing to
    /// diverge from, flagging it would only take it out of routing.
    lone: NeverForkChoice,
    members: AtomicUsize,
}

impl ForkGroup {
    pub fn new(chain: TargetBlockchain) -> Arc<Self> {
        let TargetBlockchain::Standard(chain_ref) = chain;
        let (choice, by_priority): (Arc<dyn ForkChoice>, _) = if is_pos(chain_ref) {
            let by_priority = Arc::new(PriorityForkChoice::new());
            (by_priority.clone(), Some(by_priority))
        } else {
            (Arc::new(DifficultyForkChoice::new()), None)
        };
        Arc::new(Self {
            chain,
            choice,
            by_priority,
            lone: NeverForkChoice::new(),
            members: AtomicUsize::new(0),
        })
    }

    /// Starts watching the member's head for forks.
    pub fn join(self: &Arc<Self>, member: ForkMember) {
        if let Some(by_priority) = &self.by_priority {
            by_priority.add_upstream(
                member.id.clone(),
                member.priority,
                Arc::clone(&member.state),
            );
        }
        self.members.fetch_add(1, Ordering::Relaxed);
        start_fork_watch(
            member.id,
            self.chain,
            member.head,
            member.state,
            Arc::clone(self) as Arc<dyn ForkChoice>,
        );
    }

    fn is_lone(&self) -> bool {
        self.members.load(Ordering::Relaxed) < 2
    }
}

impl ForkChoice for ForkGroup {
    fn submit(&self, block: &BlockContainer, upstream_id: &UpstreamId) -> Option<ForkStatus> {
        // The actual choice sees every block, even while its verdict doesn't
        // count, so it already knows the chain when a second upstream joins.
        let status = self.choice.submit(block, upstream_id);
        let lone_status = self.lone.submit(block, upstream_id);
        if self.is_lone() { lone_status } else { status }
    }

    fn name(&self) -> &'static str {
        if self.is_lone() {
            self.lone.name()
        } else {
            self.choice.name()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::BlockId;
    use crate::upstream::head::CurrentHead;
    use crate::upstream::id::test_id;
    use crate::upstream::state::UpstreamState;
    use emerald_api::proto::common::ChainRef;

    const RECOGNIZED: u64 = 0;

    fn block_id(branch: u64, height: u64) -> BlockId {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&branch.to_be_bytes());
        bytes[24..].copy_from_slice(&height.to_be_bytes());
        BlockId::from_bytes(bytes)
    }

    fn block(branch: u64, parent_branch: u64, height: u64, difficulty: u64) -> BlockContainer {
        BlockContainer {
            hash: block_id(branch, height),
            height,
            parent_hash: Some(block_id(parent_branch, height - 1)),
            total_difficulty: alloy::primitives::U256::from(difficulty),
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        }
    }

    fn recognized(height: u64) -> BlockContainer {
        block(RECOGNIZED, RECOGNIZED, height, height)
    }

    /// Healthy (no lag, past block 0), so it can act as the reference.
    fn join(group: &Arc<ForkGroup>, id: &str, priority: i32) {
        let state = Arc::new(UpstreamState::new());
        state.update(0, Some(100));
        group.join(ForkMember {
            id: test_id(id),
            priority,
            head: Arc::new(CurrentHead::new()),
            state,
        });
    }

    #[tokio::test]
    async fn lone_upstream_is_never_forked() {
        let group = ForkGroup::new(TargetBlockchain::from(ChainRef::ChainEthereum));
        join(&group, "top", 100);

        assert_eq!(
            group.submit(&recognized(1), &test_id("top")),
            Some(ForkStatus::New)
        );
        // Another chain at the same height, which a lone upstream can't be
        // judged by
        assert_eq!(
            group.submit(&block(1, RECOGNIZED, 1, 1), &test_id("top")),
            Some(ForkStatus::Equal)
        );
        assert_eq!(group.name(), "Never");
    }

    #[tokio::test]
    async fn pos_upstream_joining_later_is_checked_against_the_known_chain() {
        let group = ForkGroup::new(TargetBlockchain::from(ChainRef::ChainEthereum));
        join(&group, "top", 100);
        for height in 1..=3 {
            group.submit(&recognized(height), &test_id("top"));
        }

        join(&group, "low", 50);
        group.submit(&recognized(1), &test_id("low"));
        group.submit(&block(2, RECOGNIZED, 2, 2), &test_id("low"));

        assert_eq!(
            group.submit(&block(2, 2, 3, 3), &test_id("low")),
            Some(ForkStatus::Rejected { common: 1 })
        );
    }

    #[tokio::test]
    async fn pow_upstream_joining_later_is_compared_by_difficulty() {
        let group = ForkGroup::new(TargetBlockchain::from(ChainRef::ChainBitcoin));
        join(&group, "first", 0);
        group.submit(&recognized(200), &test_id("first"));

        join(&group, "second", 0);

        assert_eq!(group.name(), "Difficulty");
        assert_eq!(
            group.submit(&recognized(100), &test_id("second")),
            Some(ForkStatus::Fallbehind)
        );
    }
}
