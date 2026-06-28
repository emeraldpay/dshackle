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
use alloy::primitives::U256;
use std::cmp::Ordering;
use std::sync::Mutex;

/// Picks the heaviest chain by cumulative difficulty. It only tracks the best
/// difficulty seen so far and never reports a fork, so a PoW upstream is never
/// taken out of rotation by it — it can only be reported as behind. Ported from
/// the legacy `DifficultyForkChoice`.
///
/// The decision is made purely on difficulty order, without checking the actual
/// block graph, exactly as the legacy implementation did.
pub struct DifficultyForkChoice {
    best: Mutex<U256>,
}

impl DifficultyForkChoice {
    pub fn new() -> Self {
        Self {
            best: Mutex::new(U256::ZERO),
        }
    }
}

impl ForkChoice for DifficultyForkChoice {
    fn submit(&self, block: &BlockContainer, _upstream_id: &str) -> ForkStatus {
        let difficulty = block.total_difficulty;
        let mut best = self.best.lock().unwrap();
        let previous = *best;
        if previous < difficulty {
            *best = difficulty;
        }
        match previous.cmp(&difficulty) {
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
    use crate::data::{BlockContainer, BlockId};

    fn block(difficulty: u64) -> BlockContainer {
        BlockContainer {
            hash: BlockId::from_bytes([0u8; 32]),
            height: 1,
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
        assert_eq!(fc.submit(&block(100), "a"), ForkStatus::New);
    }

    #[test]
    fn higher_difficulty_is_new() {
        let fc = DifficultyForkChoice::new();
        fc.submit(&block(100), "a");
        assert_eq!(fc.submit(&block(200), "b"), ForkStatus::New);
    }

    #[test]
    fn same_difficulty_is_equal() {
        let fc = DifficultyForkChoice::new();
        fc.submit(&block(100), "a");
        assert_eq!(fc.submit(&block(100), "b"), ForkStatus::Equal);
    }

    #[test]
    fn lower_difficulty_is_fallbehind() {
        let fc = DifficultyForkChoice::new();
        fc.submit(&block(200), "a");
        assert_eq!(fc.submit(&block(100), "b"), ForkStatus::Fallbehind);
    }

    #[test]
    fn never_rejects() {
        let fc = DifficultyForkChoice::new();
        fc.submit(&block(200), "a");
        assert!(fc.submit(&block(1), "b").is_ok());
    }
}
