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

//! Difficulty-based fork choice for Proof-of-Work chains.

use super::{ForkChoice, ForkStatus};
use crate::data::BlockContainer;
use crate::upstream::id::UpstreamId;
use alloy::primitives::U256;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Mutex;

/// Picks the heaviest chain by cumulative difficulty. It only tracks the best
/// difficulty seen so far and never reports a fork, so a PoW upstream is never
/// taken out of rotation by it — it can only be reported as behind. Ported from
/// the legacy `DifficultyForkChoice`.
///
/// The decision is made purely on difficulty order, without checking the actual
/// block graph, exactly as the legacy implementation did. A block that carries
/// no difficulty (some sources omit `totalDifficulty`) is a config smell on a
/// PoW chain — reported as an error once per upstream — and difficulty order is
/// only meaningful when both sides carry it, so any zero on either side falls
/// back to height order. Comparing a real difficulty against a missing one
/// would let a single stale-but-weighted block outrank every current
/// weightless tip and freeze the status here.
pub struct DifficultyForkChoice {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    best_difficulty: U256,
    best_height: u64,
    /// Upstreams already reported for missing difficulty, to log once instead
    /// of every block.
    reported_zero: HashSet<UpstreamId>,
}

impl DifficultyForkChoice {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }
}

impl ForkChoice for DifficultyForkChoice {
    fn submit(&self, block: &BlockContainer, upstream_id: &UpstreamId) -> ForkStatus {
        let difficulty = block.total_difficulty;
        let mut state = self.state.lock().unwrap();
        if difficulty == U256::ZERO && state.reported_zero.insert(upstream_id.clone()) {
            tracing::error!(
                upstream = %upstream_id,
                "Block reports no total difficulty on a PoW chain; the node \
                 may have pruned it or the source omits it — falling back to \
                 height ordering"
            );
        }
        let order = if state.best_difficulty != U256::ZERO && difficulty != U256::ZERO {
            state.best_difficulty.cmp(&difficulty)
        } else {
            state.best_height.cmp(&block.height)
        };
        if order == Ordering::Less {
            state.best_difficulty = difficulty;
            state.best_height = block.height;
        }
        match order {
            Ordering::Greater => ForkStatus::Fallbehind,
            Ordering::Less => ForkStatus::New,
            Ordering::Equal => ForkStatus::Equal,
        }
    }

    fn name(&self) -> &'static str {
        "Difficulty"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::id::test_id;

    use crate::data::{BlockContainer, BlockId};

    fn block(difficulty: u64) -> BlockContainer {
        block_at(difficulty, 1)
    }

    fn block_at(difficulty: u64, height: u64) -> BlockContainer {
        BlockContainer {
            hash: BlockId::from_bytes([0u8; 32]),
            height,
            parent_hash: None,
            total_difficulty: U256::from(difficulty),
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        }
    }

    #[test]
    fn first_block_is_new() {
        let fc = DifficultyForkChoice::new();
        assert_eq!(fc.submit(&block(100), &test_id("up-a")), ForkStatus::New);
    }

    #[test]
    fn higher_difficulty_is_new() {
        let fc = DifficultyForkChoice::new();
        fc.submit(&block(100), &test_id("up-a"));
        assert_eq!(fc.submit(&block(200), &test_id("up-b")), ForkStatus::New);
    }

    #[test]
    fn same_difficulty_is_equal() {
        let fc = DifficultyForkChoice::new();
        fc.submit(&block(100), &test_id("up-a"));
        assert_eq!(fc.submit(&block(100), &test_id("up-b")), ForkStatus::Equal);
    }

    #[test]
    fn lower_difficulty_is_fallbehind() {
        let fc = DifficultyForkChoice::new();
        fc.submit(&block(200), &test_id("up-a"));
        assert_eq!(
            fc.submit(&block(100), &test_id("up-b")),
            ForkStatus::Fallbehind
        );
    }

    #[test]
    fn never_rejects() {
        let fc = DifficultyForkChoice::new();
        fc.submit(&block(200), &test_id("up-a"));
        assert!(fc.submit(&block(1), &test_id("up-b")).is_ok());
    }

    #[test]
    fn zero_difficulty_falls_back_to_height_order() {
        // Sources that omit totalDifficulty parse as zero; the chain's head
        // must still advance, so height decides.
        let fc = DifficultyForkChoice::new();
        assert_eq!(
            fc.submit(&block_at(0, 100), &test_id("up-a")),
            ForkStatus::New
        );
        assert_eq!(
            fc.submit(&block_at(0, 101), &test_id("up-a")),
            ForkStatus::New
        );
        assert_eq!(
            fc.submit(&block_at(0, 101), &test_id("up-b")),
            ForkStatus::Equal
        );
        assert_eq!(
            fc.submit(&block_at(0, 99), &test_id("up-b")),
            ForkStatus::Fallbehind
        );
    }

    #[test]
    fn stale_weighted_block_cannot_freeze_a_weightless_chain() {
        // The live upstreams omit totalDifficulty (height fallback active); a
        // lagging upstream reports a weighted but old block. Difficulty must
        // not be compared against "unknown", or that one stale block would
        // outrank every current tip from then on.
        let fc = DifficultyForkChoice::new();
        fc.submit(&block_at(0, 1000), &test_id("up-a"));
        assert_eq!(
            fc.submit(&block_at(500, 500), &test_id("up-b")),
            ForkStatus::Fallbehind
        );
        assert_eq!(
            fc.submit(&block_at(0, 1001), &test_id("up-a")),
            ForkStatus::New
        );
    }
}
