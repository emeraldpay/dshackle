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

//! A fork choice that never reports a fork.

use super::{ForkChoice, ForkStatus};
use crate::data::BlockContainer;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as MemOrder};

/// Tracks only the best height seen and never reports a fork. The default for
/// chains without a real fork strategy, and a stand-in in tests. Ported from
/// the legacy `NeverForkChoice`.
pub struct NeverForkChoice {
    height: AtomicU64,
}

impl NeverForkChoice {
    pub fn new() -> Self {
        Self {
            height: AtomicU64::new(0),
        }
    }
}

impl ForkChoice for NeverForkChoice {
    fn submit(&self, block: &BlockContainer, _upstream_id: &str) -> ForkStatus {
        let previous = self.height.fetch_max(block.height, MemOrder::Relaxed);
        match previous.cmp(&block.height) {
            Ordering::Greater => ForkStatus::Fallbehind,
            Ordering::Less => ForkStatus::New,
            Ordering::Equal => ForkStatus::Equal,
        }
    }

    fn name(&self) -> &'static str {
        "Never"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::BlockId;

    fn block(height: u64) -> BlockContainer {
        BlockContainer {
            hash: BlockId::from_bytes([0u8; 32]),
            height,
            parent_hash: None,
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        }
    }

    #[test]
    fn tracks_height_and_never_rejects() {
        let fc = NeverForkChoice::new();
        assert_eq!(fc.submit(&block(10), "a"), ForkStatus::New);
        assert_eq!(fc.submit(&block(10), "b"), ForkStatus::Equal);
        assert_eq!(fc.submit(&block(5), "c"), ForkStatus::Fallbehind);
        assert!(fc.submit(&block(1), "d").is_ok());
    }
}
