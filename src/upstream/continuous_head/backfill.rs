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

//! Recovering the blocks between the announced history and a new tip.

use super::HISTORY_LIMIT;
use super::history::HeadHistory;
use super::source::BlockSource;
use crate::data::{BlockContainer, BlockId};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Limit for one block fetch. A block that slow to fetch is better given up
/// on: the tip is then announced with a gap rather than late.
const FETCH_TIMEOUT: Duration = Duration::from_secs(2);

/// Limit for the whole walk. Every subscriber of the chain waits on it, so a
/// slow upstream answering a deep walk block by block must not hold the head
/// back for long.
const WALK_TIMEOUT: Duration = Duration::from_secs(5);

impl HeadHistory {
    /// The branch from the history to `tip`, oldest first, ready for
    /// [`extend`](HeadHistory::extend).
    ///
    /// Walks back from `tip` by parent hash, fetching each missing ancestor,
    /// until reaching a block whose parent the history holds. The walk only
    /// goes as deep as the history: an ancestor below it couldn't be attached
    /// anyway. When the walk can't finish, what it found so far is returned
    /// and `extend` announces it best effort.
    pub async fn branch_to(
        &self,
        tip: Arc<BlockContainer>,
        source: Option<&dyn BlockSource>,
    ) -> Vec<Arc<BlockContainer>> {
        let (Some(source), Some(history_tip), Some(lowest)) =
            (source, self.tip(), self.first_height())
        else {
            return vec![tip];
        };
        // Too many blocks missing to fetch them all within the limit; or a
        // stale tip, which `extend` ignores, so there's nothing to fetch for.
        if tip.height > history_tip.height + HISTORY_LIMIT as u64 || tip.height < lowest {
            return vec![tip];
        }

        let fetcher = Fetcher {
            source,
            deadline: Instant::now() + WALK_TIMEOUT,
        };
        let mut branch = vec![fetcher.with_parent(tip).await];
        while branch.len() <= HISTORY_LIMIT {
            let oldest = branch.last().expect("the branch starts with the tip");
            if self.contains(&oldest.hash) || oldest.height <= lowest {
                break;
            }
            let Some(parent_hash) = oldest.parent_hash else {
                break;
            };
            if self.contains(&parent_hash) {
                break;
            }
            match fetcher.block(&parent_hash, oldest.height - 1).await {
                Some(parent) if parent.height + 1 == oldest.height => {
                    branch.push(Arc::new(parent));
                }
                _ => break,
            }
        }
        branch.reverse();
        branch
    }
}

/// Fetches the blocks of one walk, within its deadline.
struct Fetcher<'a> {
    source: &'a dyn BlockSource,
    deadline: Instant,
}

impl Fetcher<'_> {
    /// A bare head (a remote Dshackle's `ChainHead`) carries no parent hash,
    /// so there is nothing to walk from; looking the block itself up gives it
    /// one.
    async fn with_parent(&self, tip: Arc<BlockContainer>) -> Arc<BlockContainer> {
        if tip.parent_hash.is_some() {
            return tip;
        }
        match self
            .block(&tip.hash, tip.height)
            .await
            .and_then(|b| b.parent_hash)
        {
            Some(parent_hash) => Arc::new(BlockContainer {
                parent_hash: Some(parent_hash),
                ..(*tip).clone()
            }),
            None => tip,
        }
    }

    /// The block, refusing an answer for a different block: an upstream may
    /// resolve the request any way it likes, and a wrong block would corrupt
    /// the branch.
    async fn block(&self, hash: &BlockId, height: u64) -> Option<BlockContainer> {
        let limit = self.deadline.min(Instant::now() + FETCH_TIMEOUT);
        let block = tokio::time::timeout_at(limit, self.source.block_by_hash(hash, height))
            .await
            .ok()??;
        (block.hash == *hash).then_some(block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::continuous_head::testing::{Chain, StoredBlocks, block};

    fn heights(branch: &[Arc<BlockContainer>]) -> Vec<(u64, u8)> {
        branch
            .iter()
            .map(|b| (b.height, b.hash.as_bytes()[0]))
            .collect()
    }

    fn history_of(blocks: &[Arc<BlockContainer>]) -> HeadHistory {
        let mut history = HeadHistory::default();
        history.extend(blocks.to_vec());
        history
    }

    #[tokio::test]
    async fn child_of_the_tip_needs_no_fetch() {
        let chain = Chain::new(1).grow(100, 3);
        let source = StoredBlocks::of(&chain);
        let history = history_of(&chain.blocks[..2]);

        let branch = history
            .branch_to(Arc::clone(&chain.blocks[2]), Some(&source))
            .await;

        assert_eq!(heights(&branch), vec![(102, 1)]);
        assert_eq!(source.fetches(), 0);
    }

    #[tokio::test]
    async fn forward_gap_is_filled() {
        let chain = Chain::new(1).grow(100, 6);
        let source = StoredBlocks::of(&chain);
        let history = history_of(&chain.blocks[..2]);

        let branch = history
            .branch_to(Arc::clone(&chain.blocks[5]), Some(&source))
            .await;

        assert_eq!(
            heights(&branch),
            vec![(102, 1), (103, 1), (104, 1), (105, 1)]
        );
    }

    #[tokio::test]
    async fn reorg_is_walked_down_to_the_fork_point() {
        let main = Chain::new(1).grow(100, 4);
        let fork = main.fork_at(101, 2).grow(101, 4);
        let source = StoredBlocks::of(&fork);
        let history = history_of(&main.blocks);

        let tip = Arc::clone(fork.blocks.last().unwrap());
        let branch = history.branch_to(tip, Some(&source)).await;

        assert_eq!(
            heights(&branch),
            vec![(101, 2), (102, 2), (103, 2), (104, 2)]
        );
    }

    #[tokio::test]
    async fn announced_tip_needs_no_fetch() {
        let chain = Chain::new(1).grow(100, 4);
        let source = StoredBlocks::of(&chain);
        let history = history_of(&chain.blocks);

        let branch = history
            .branch_to(Arc::clone(&chain.blocks[1]), Some(&source))
            .await;

        assert_eq!(heights(&branch), vec![(101, 1)]);
        assert_eq!(source.fetches(), 0);
    }

    #[tokio::test]
    async fn failed_fetch_returns_what_was_found() {
        let chain = Chain::new(1).grow(100, 6);
        let mut source = StoredBlocks::of(&chain);
        source.forget(103);
        let history = history_of(&chain.blocks[..2]);

        let branch = history
            .branch_to(Arc::clone(&chain.blocks[5]), Some(&source))
            .await;

        assert_eq!(heights(&branch), vec![(104, 1), (105, 1)]);
    }

    #[tokio::test]
    async fn fetched_block_of_another_hash_is_refused() {
        let chain = Chain::new(1).grow(100, 4);
        let other = Chain::new(9).grow(100, 4);
        let source = StoredBlocks::answering_with(&other);
        let history = history_of(&chain.blocks[..2]);

        let branch = history
            .branch_to(Arc::clone(&chain.blocks[3]), Some(&source))
            .await;

        assert_eq!(heights(&branch), vec![(103, 1)]);
    }

    #[tokio::test]
    async fn walk_goes_no_deeper_than_the_history() {
        let main = Chain::new(1).grow(100, 4);
        let fork = main.fork_at(95, 2).grow(95, 10);
        let source = StoredBlocks::of(&fork);
        let history = history_of(&main.blocks);

        let tip = Arc::clone(fork.blocks.last().unwrap());
        let branch = history.branch_to(tip, Some(&source)).await;

        assert_eq!(branch.first().unwrap().height, 100);
    }

    #[tokio::test]
    async fn too_far_ahead_is_not_walked() {
        let chain = Chain::new(1).grow(100, HISTORY_LIMIT as u64 + 10);
        let source = StoredBlocks::of(&chain);
        let history = history_of(&chain.blocks[..2]);

        let tip = Arc::clone(chain.blocks.last().unwrap());
        let branch = history.branch_to(tip, Some(&source)).await;

        assert_eq!(branch.len(), 1);
        assert_eq!(source.fetches(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_source_is_given_up_on_with_what_was_found() {
        let chain = Chain::new(1).grow(100, 14);
        let source = StoredBlocks::of(&chain).answering_after(Duration::from_secs(1));
        let history = history_of(&chain.blocks[..2]);
        let started = Instant::now();

        let tip = Arc::clone(chain.blocks.last().unwrap());
        let branch = history.branch_to(tip, Some(&source)).await;

        assert!(started.elapsed() <= WALK_TIMEOUT);
        // The newest blocks fetched in time; the gap below them stays.
        assert_eq!(branch.last().unwrap().height, 113);
        assert!(branch.len() > 1);
        assert!(branch.first().unwrap().height > 102);
    }

    #[tokio::test(start_paused = true)]
    async fn stuck_fetch_is_given_up_on() {
        let chain = Chain::new(1).grow(100, 4);
        let source = StoredBlocks::of(&chain).answering_after(Duration::from_secs(60));
        let history = history_of(&chain.blocks[..2]);
        let started = Instant::now();

        let branch = history
            .branch_to(Arc::clone(&chain.blocks[3]), Some(&source))
            .await;

        assert_eq!(started.elapsed(), FETCH_TIMEOUT);
        assert_eq!(heights(&branch), vec![(103, 1)]);
    }

    #[tokio::test]
    async fn parentless_tip_learns_its_parent() {
        let main = Chain::new(1).grow(100, 3);
        let fork = main.fork_at(101, 2).grow(101, 2);
        let source = StoredBlocks::of(&fork);
        let history = history_of(&main.blocks);
        let mut bare = (**fork.blocks.last().unwrap()).clone();
        bare.parent_hash = None;

        let branch = history.branch_to(Arc::new(bare), Some(&source)).await;

        assert_eq!(heights(&branch), vec![(101, 2), (102, 2)]);
        assert!(branch[1].parent_hash.is_some());
    }

    #[tokio::test]
    async fn stale_tip_needs_no_fetch() {
        let chain = Chain::new(1).grow(100, 4);
        let source = StoredBlocks::of(&chain);
        let history = history_of(&chain.blocks);
        let mut stale = block(50, 2);
        stale.parent_hash = None;

        let branch = history.branch_to(Arc::new(stale), Some(&source)).await;

        assert_eq!(heights(&branch), vec![(50, 2)]);
        assert_eq!(source.fetches(), 0);
    }

    #[tokio::test]
    async fn without_a_source_the_tip_is_returned_alone() {
        let chain = Chain::new(1).grow(100, 4);
        let history = history_of(&chain.blocks[..2]);

        let branch = history.branch_to(Arc::clone(&chain.blocks[3]), None).await;

        assert_eq!(heights(&branch), vec![(103, 1)]);
    }
}
