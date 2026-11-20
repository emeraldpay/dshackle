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

//! The chain's head as a continuous sequence of blocks, for the head
//! subscriptions (gRPC `SubscribeHead`, `eth_subscribe("newHeads")`).
//!
//! The [merged head](crate::upstream::merged_head) only announces the tips the
//! upstreams report: a polled upstream skips the blocks between its polls, and
//! after a reorg the new tip may be several blocks past the fork. A subscriber
//! of the raw tips can't tell a reorg from a gap. [`ContinuousHead`] makes
//! every announced block a child of the previous one: the skipped blocks are
//! fetched by parent hash, and on a reorg the sequence steps back to the first
//! replaced height, with [`SequencedBlock::reorg`] telling how many announced
//! blocks were replaced. It's best effort within [`HISTORY_LIMIT`] blocks:
//! anything deeper, or a block that can't be fetched, leaves a gap.
//!
//! One task per chain walks the gaps and keeps the shared history; each
//! subscriber follows that history with a cursor of its own, so a subscriber
//! that falls behind catches up from the history instead of skipping blocks.

mod backfill;
mod history;
pub mod source;
#[cfg(test)]
mod testing;

pub use history::SequencedBlock;

use crate::data::BlockContainer;
use crate::upstream::merged_head::MergedHead;
use futures::Stream;
use history::HeadHistory;
use source::BlockSource;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};

/// How many recent blocks are kept to attach new ones to, and so the deepest
/// reorg or the longest gap that can be filled. Normally a reorg is a block or
/// two; this covers well beyond that.
pub const HISTORY_LIMIT: usize = 16;

/// A subscription to the continuous head.
pub type HeadStream = Pin<Box<dyn Stream<Item = SequencedBlock> + Send>>;

/// Where a new subscription starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartAt {
    /// With the current head, so the subscriber knows the chain state right
    /// away (the legacy `SubscribeHead`).
    CurrentHead,
    /// With the next new block, as a node's `newHeads` does.
    NextHead,
}

/// The continuous head of one chain.
pub struct ContinuousHead {
    history: watch::Receiver<HeadHistory>,
}

impl ContinuousHead {
    /// Follow `merged` for as long as it exists, fetching the skipped blocks
    /// from `source` (without one, gaps are announced as they come).
    pub fn start(merged: &MergedHead, source: Option<Arc<dyn BlockSource>>) -> Arc<Self> {
        let (sender, history) = watch::channel(HeadHistory::default());
        tokio::spawn(follow(merged.subscribe(), source, sender));
        Arc::new(Self { history })
    }

    pub fn subscribe(&self, start: StartAt) -> HeadStream {
        let mut shared = self.history.clone();
        let seen = shared.borrow_and_update().clone();
        let pending = match start {
            StartAt::CurrentHead => seen
                .tip()
                .map(|tip| SequencedBlock {
                    block: Arc::clone(tip),
                    reorg: 0,
                })
                .into_iter()
                .collect(),
            StartAt::NextHead => VecDeque::new(),
        };
        let cursor = Cursor {
            shared,
            seen,
            pending,
        };
        Box::pin(futures::stream::unfold(cursor, Cursor::next))
    }
}

/// A subscriber's position: the blocks it was given, diffed against the
/// shared history whenever that changes. The diff is the same
/// [`extend`](HeadHistory::extend) the shared history is built with, so a
/// subscriber missing a few updates gets exactly the blocks it lacks, and a
/// reorg is counted against what *it* was given.
struct Cursor {
    shared: watch::Receiver<HeadHistory>,
    seen: HeadHistory,
    pending: VecDeque<SequencedBlock>,
}

impl Cursor {
    async fn next(mut self) -> Option<(SequencedBlock, Self)> {
        loop {
            if let Some(next) = self.pending.pop_front() {
                return Some((next, self));
            }
            self.shared.changed().await.ok()?;
            let latest: Vec<_> = self.shared.borrow_and_update().blocks().cloned().collect();
            self.pending.extend(self.seen.extend(latest));
        }
    }
}

/// Attach each tip the merged head announces to the shared history.
///
/// Tips are taken one by one rather than skipping to the newest: an
/// intermediate tip is usually the parent of the next one, and attaching it
/// now saves fetching it later. A lagged receiver loses some, which the walk
/// then recovers.
///
/// Gaps are only filled while someone is subscribed: the history then simply
/// restarts at the next tip, and an idle chain costs the upstreams no calls.
async fn follow(
    mut tips: broadcast::Receiver<Arc<BlockContainer>>,
    source: Option<Arc<dyn BlockSource>>,
    shared: watch::Sender<HeadHistory>,
) {
    let mut history = HeadHistory::default();
    loop {
        let tip = match tips.recv().await {
            Ok(tip) => tip,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return,
        };
        // `ContinuousHead` itself holds one receiver; every subscriber adds one.
        let subscribed = shared.receiver_count() > 1;
        let source = source.as_deref().filter(|_| subscribed);
        let branch = history.branch_to(tip, source).await;
        if !history.extend(branch).is_empty() {
            shared.send_replace(history.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::merged_head::MergeOrder;
    use futures::StreamExt;
    use std::time::Duration;
    use testing::{Chain, StoredBlocks};

    async fn next(stream: &mut HeadStream) -> (u64, u8, u64) {
        let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("no block announced")
            .expect("stream ended");
        (
            event.block.height,
            event.block.hash.as_bytes()[0],
            event.reorg,
        )
    }

    async fn nothing_next(stream: &mut HeadStream) {
        let next = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(next.is_err(), "unexpected block announced");
    }

    fn feed(merged: &MergedHead, blocks: &[Arc<BlockContainer>]) {
        for block in blocks {
            merged.feed(0, Arc::clone(block));
        }
    }

    fn head_with(source: StoredBlocks) -> (Arc<MergedHead>, Arc<ContinuousHead>) {
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let source: Arc<dyn BlockSource> = Arc::new(source);
        let head = ContinuousHead::start(&merged, Some(source));
        (merged, head)
    }

    /// Let the follower task drain what was fed so far.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    #[tokio::test]
    async fn announces_blocks_in_order() {
        let chain = Chain::new(1).grow(100, 3);
        let (merged, head) = head_with(StoredBlocks::of(&chain));
        let mut stream = head.subscribe(StartAt::NextHead);

        feed(&merged, &chain.blocks);

        assert_eq!(next(&mut stream).await, (100, 1, 0));
        assert_eq!(next(&mut stream).await, (101, 1, 0));
        assert_eq!(next(&mut stream).await, (102, 1, 0));
    }

    #[tokio::test]
    async fn fills_the_blocks_between_polls() {
        let chain = Chain::new(1).grow(100, 5);
        let (merged, head) = head_with(StoredBlocks::of(&chain));
        let mut stream = head.subscribe(StartAt::NextHead);

        feed(&merged, &chain.blocks[..1]);
        assert_eq!(next(&mut stream).await, (100, 1, 0));
        feed(&merged, &chain.blocks[4..]);

        assert_eq!(next(&mut stream).await, (101, 1, 0));
        assert_eq!(next(&mut stream).await, (102, 1, 0));
        assert_eq!(next(&mut stream).await, (103, 1, 0));
        assert_eq!(next(&mut stream).await, (104, 1, 0));
    }

    #[tokio::test]
    async fn reorg_steps_back_to_the_fork_and_counts_the_replaced_blocks() {
        let main = Chain::new(1).grow(100, 3);
        let fork = main.fork_at(101, 2).grow(101, 3);
        let (merged, head) = head_with(StoredBlocks::with(&[&main, &fork]));
        let mut stream = head.subscribe(StartAt::NextHead);

        feed(&merged, &main.blocks);
        for _ in 0..3 {
            next(&mut stream).await;
        }
        // Only the new tip is announced, two blocks past the fork.
        feed(&merged, &fork.from(103));

        assert_eq!(next(&mut stream).await, (101, 2, 2));
        assert_eq!(next(&mut stream).await, (102, 2, 0));
        assert_eq!(next(&mut stream).await, (103, 2, 0));
    }

    #[tokio::test]
    async fn head_regressing_and_recovering_is_not_a_reorg() {
        // A higher-priority upstream coming up behind the tip pulls the merged
        // head back to an already announced block.
        let chain = Chain::new(1).grow(100, 4);
        let (merged, head) = head_with(StoredBlocks::of(&chain));
        let mut stream = head.subscribe(StartAt::NextHead);

        feed(&merged, &chain.blocks[..3]);
        for _ in 0..3 {
            next(&mut stream).await;
        }
        merged.feed(10, Arc::clone(&chain.blocks[1]));
        nothing_next(&mut stream).await;
        merged.feed(10, Arc::clone(&chain.blocks[2]));
        merged.feed(10, Arc::clone(&chain.blocks[3]));

        assert_eq!(next(&mut stream).await, (103, 1, 0));
        nothing_next(&mut stream).await;
    }

    #[tokio::test]
    async fn upstream_far_behind_taking_over_is_not_a_reorg() {
        // A higher-priority node resyncing far behind wins the merge for a
        // moment (the PoS merge restarts from a higher priority's view).
        let chain = Chain::new(1).grow(100, 5);
        let (merged, head) = head_with(StoredBlocks::of(&chain));
        let mut stream = head.subscribe(StartAt::NextHead);

        feed(&merged, &chain.blocks[..4]);
        for _ in 0..4 {
            next(&mut stream).await;
        }
        merged.feed(10, Arc::new(testing::block(15, 2)));
        nothing_next(&mut stream).await;
        feed(&merged, &chain.blocks[4..]);

        assert_eq!(next(&mut stream).await, (104, 1, 0));
    }

    #[tokio::test]
    async fn late_subscriber_catches_up_without_gaps() {
        let main = Chain::new(1).grow(100, 4);
        let fork = main.fork_at(102, 2).grow(102, 3);
        let (merged, head) = head_with(StoredBlocks::with(&[&main, &fork]));

        feed(&merged, &main.blocks[..1]);
        settle().await;
        let mut stream = head.subscribe(StartAt::NextHead);
        // Not reading while the chain moves on and reorgs.
        feed(&merged, &main.blocks[1..]);
        settle().await;
        feed(&merged, &fork.from(104));
        settle().await;

        // It never saw the replaced blocks, so for it there was no reorg.
        assert_eq!(next(&mut stream).await, (101, 1, 0));
        assert_eq!(next(&mut stream).await, (102, 2, 0));
        assert_eq!(next(&mut stream).await, (103, 2, 0));
        assert_eq!(next(&mut stream).await, (104, 2, 0));
    }

    #[tokio::test]
    async fn slow_subscriber_is_told_about_the_reorg_it_missed() {
        let main = Chain::new(1).grow(100, 4);
        let fork = main.fork_at(102, 2).grow(102, 3);
        let (merged, head) = head_with(StoredBlocks::with(&[&main, &fork]));
        let mut stream = head.subscribe(StartAt::NextHead);

        feed(&merged, &main.blocks);
        for _ in 0..4 {
            next(&mut stream).await;
        }
        // The whole reorg happens before the subscriber reads again.
        feed(&merged, &fork.from(102));
        settle().await;

        assert_eq!(next(&mut stream).await, (102, 2, 2));
        assert_eq!(next(&mut stream).await, (103, 2, 0));
        assert_eq!(next(&mut stream).await, (104, 2, 0));
    }

    #[tokio::test]
    async fn starting_at_the_current_head_announces_it_first() {
        let chain = Chain::new(1).grow(100, 3);
        let (merged, head) = head_with(StoredBlocks::of(&chain));

        feed(&merged, &chain.blocks[..2]);
        settle().await;
        let mut stream = head.subscribe(StartAt::CurrentHead);
        feed(&merged, &chain.blocks[2..]);

        assert_eq!(next(&mut stream).await, (101, 1, 0));
        assert_eq!(next(&mut stream).await, (102, 1, 0));
    }

    #[tokio::test]
    async fn starting_at_the_next_head_skips_the_current_one() {
        let chain = Chain::new(1).grow(100, 3);
        let (merged, head) = head_with(StoredBlocks::of(&chain));

        feed(&merged, &chain.blocks[..2]);
        settle().await;
        let mut stream = head.subscribe(StartAt::NextHead);
        feed(&merged, &chain.blocks[2..]);

        assert_eq!(next(&mut stream).await, (102, 1, 0));
    }

    #[tokio::test]
    async fn gaps_are_not_fetched_without_subscribers() {
        let chain = Chain::new(1).grow(100, 7);
        let source = Arc::new(StoredBlocks::of(&chain));
        let merged = Arc::new(MergedHead::new(MergeOrder::Priority));
        let head = ContinuousHead::start(&merged, Some(Arc::clone(&source) as _));

        feed(&merged, &chain.blocks[..1]);
        feed(&merged, &chain.blocks[4..5]);
        settle().await;
        assert_eq!(source.fetches(), 0);

        // A subscriber arriving later gets the chain from there on, gap-free.
        let mut stream = head.subscribe(StartAt::CurrentHead);
        feed(&merged, &chain.blocks[6..]);
        assert_eq!(next(&mut stream).await, (104, 1, 0));
        assert_eq!(next(&mut stream).await, (105, 1, 0));
        assert_eq!(next(&mut stream).await, (106, 1, 0));
    }

    #[tokio::test]
    async fn unrecoverable_gap_is_announced_as_is() {
        let chain = Chain::new(1).grow(100, 5);
        let (merged, head) = head_with(StoredBlocks::of(&Chain::new(1)));
        let mut stream = head.subscribe(StartAt::NextHead);

        feed(&merged, &chain.blocks[..1]);
        assert_eq!(next(&mut stream).await, (100, 1, 0));
        feed(&merged, &chain.blocks[4..]);

        assert_eq!(next(&mut stream).await, (104, 1, 0));
    }
}
