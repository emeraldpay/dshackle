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

//! Chains of linked blocks for the tests of the continuous head.

use super::source::BlockSource;
use crate::data::{BlockContainer, BlockId};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// The id of the block at `height` on `branch`: the branch in the first byte
/// (so tests can tell branches apart), the height after it.
pub fn block_id(height: u64, branch: u8) -> BlockId {
    let mut hash = [0u8; 32];
    hash[0] = branch;
    hash[1..9].copy_from_slice(&height.to_be_bytes());
    BlockId::from_bytes(hash)
}

/// A block on `branch` whose parent is the previous height on the same branch.
pub fn block(height: u64, branch: u8) -> BlockContainer {
    BlockContainer {
        hash: block_id(height, branch),
        height,
        parent_hash: Some(block_id(height - 1, branch)),
        total_difficulty: alloy::primitives::U256::ZERO,
        timestamp: jiff::Timestamp::UNIX_EPOCH,
        transaction_hashes: vec![],
        json: None,
        header_json: None,
    }
}

/// A run of parent-linked blocks, oldest first.
#[derive(Clone)]
pub struct Chain {
    pub blocks: Vec<Arc<BlockContainer>>,
    branch: u8,
}

impl Chain {
    pub fn new(branch: u8) -> Self {
        Self {
            blocks: Vec::new(),
            branch,
        }
    }

    /// The chain with `count` more blocks from `height` on its branch, the
    /// first of them a child of the chain's last block.
    pub fn grow(&self, height: u64, count: u64) -> Self {
        let mut chain = self.clone();
        for h in height..height + count {
            let mut next = block(h, chain.branch);
            if let Some(last) = chain.blocks.last() {
                next.parent_hash = Some(last.hash);
            }
            chain.blocks.push(Arc::new(next));
        }
        chain
    }

    /// The part of this chain below `height`, continuing on another branch.
    pub fn fork_at(&self, height: u64, branch: u8) -> Self {
        Self {
            blocks: self
                .blocks
                .iter()
                .filter(|b| b.height < height)
                .cloned()
                .collect(),
            branch,
        }
    }

    /// The blocks at `height` and above.
    pub fn from(&self, height: u64) -> Vec<Arc<BlockContainer>> {
        self.blocks
            .iter()
            .filter(|b| b.height >= height)
            .cloned()
            .collect()
    }
}

/// A block source answering from a fixed set of blocks, counting the lookups.
pub struct StoredBlocks {
    blocks: HashMap<BlockId, BlockContainer>,
    /// Answer every lookup with this block instead, whatever was asked.
    wrong_answer: Option<BlockContainer>,
    /// How long each lookup takes.
    delay: Duration,
    /// How many of the first lookups find nothing, as from a node that
    /// doesn't have the block yet.
    misses: usize,
    fetches: AtomicUsize,
}

impl StoredBlocks {
    pub fn of(chain: &Chain) -> Self {
        Self::with(&[chain])
    }

    pub fn with(chains: &[&Chain]) -> Self {
        Self {
            blocks: chains
                .iter()
                .flat_map(|c| c.blocks.iter())
                .map(|b| (b.hash, (**b).clone()))
                .collect(),
            wrong_answer: None,
            delay: Duration::ZERO,
            misses: 0,
            fetches: AtomicUsize::new(0),
        }
    }

    pub fn answering_with(other: &Chain) -> Self {
        Self {
            blocks: HashMap::new(),
            wrong_answer: Some((*other.blocks[0]).clone()),
            delay: Duration::ZERO,
            misses: 0,
            fetches: AtomicUsize::new(0),
        }
    }

    pub fn missing_first(self, misses: usize) -> Self {
        Self { misses, ..self }
    }

    pub fn answering_after(self, delay: Duration) -> Self {
        Self { delay, ..self }
    }

    pub fn forget(&mut self, height: u64) {
        self.blocks.retain(|_, b| b.height != height);
    }

    pub fn fetches(&self) -> usize {
        self.fetches.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl BlockSource for StoredBlocks {
    async fn block_by_hash(&self, hash: &BlockId, _height: u64) -> Option<BlockContainer> {
        let attempt = self.fetches.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.delay).await;
        if attempt < self.misses {
            return None;
        }
        if let Some(wrong) = &self.wrong_answer {
            return Some(wrong.clone());
        }
        self.blocks.get(hash).cloned()
    }
}
