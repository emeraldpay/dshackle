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

//! Cross-upstream fork detection.
//!
//! Each upstream reports its own latest block, but the upstreams may disagree:
//! one can sit on a stale or orphaned chain. A [`ForkChoice`] inspects each new
//! block against what the other upstreams have seen and classifies the upstream
//! with a [`ForkStatus`]. A forked upstream is taken out of rotation by its
//! [`ForkWatch`](watch::start_fork_watch), which records the verdict on the
//! upstream's [`UpstreamState`](crate::upstream::state::UpstreamState).
//!
//! Two strategies, picked by consensus (see [`is_pos`]):
//! - [`DifficultyForkChoice`] for Proof-of-Work — the heaviest chain wins.
//! - [`PriorityForkChoice`] for Proof-of-Stake — divergence from the
//!   highest-priority healthy upstream is a fork.

mod difficulty;
mod never;
mod priority;
mod watch;

pub use difficulty::DifficultyForkChoice;
pub use never::NeverForkChoice;
pub use priority::PriorityForkChoice;
pub use watch::{ForkMember, start_fork_watch};

use crate::data::BlockContainer;
use crate::upstream::id::UpstreamId;
use emerald_api::proto::common::ChainRef;

/// How one upstream's latest block relates to the chain's recognized head.
/// Ported from the legacy `ForkChoice.Status`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForkStatus {
    /// Reached a new good block ahead of the others.
    New,
    /// Reached a new block the other upstreams haven't seen yet; it may still
    /// be replaced.
    Outrun,
    /// On the same block as the recognized head.
    Equal,
    /// On a known block, but behind the recognized head.
    Fallbehind,
    /// On a chain that diverges from the recognized one — forked.
    Rejected,
}

impl ForkStatus {
    /// Every status, for reports that must cover the full range.
    pub const ALL: [ForkStatus; 5] = [
        ForkStatus::New,
        ForkStatus::Outrun,
        ForkStatus::Equal,
        ForkStatus::Fallbehind,
        ForkStatus::Rejected,
    ];

    /// `true` unless the upstream is forked away from the recognized chain.
    pub fn is_ok(&self) -> bool {
        !matches!(self, ForkStatus::Rejected)
    }

    /// The `status` metric label — the legacy enum constant name.
    pub fn metrics_label(&self) -> &'static str {
        match self {
            ForkStatus::New => "NEW",
            ForkStatus::Outrun => "OUTRUN",
            ForkStatus::Equal => "EQUAL",
            ForkStatus::Fallbehind => "FALLBEHIND",
            ForkStatus::Rejected => "REJECTED",
        }
    }
}

/// Evaluates whether an upstream's latest block keeps it on the recognized
/// chain. One instance is shared per blockchain; every upstream's fork watcher
/// submits its blocks to it, so the choice sees the whole picture.
pub trait ForkChoice: Send + Sync {
    /// Classify `block` reported by the upstream identified by `upstream_id`.
    fn submit(&self, block: &BlockContainer, upstream_id: &UpstreamId) -> ForkStatus;

    /// Name of the strategy, for logging.
    fn name(&self) -> &'static str;
}

/// Whether a chain uses Proof-of-Stake consensus. PoS chains can't be ordered
/// by cumulative difficulty, so they use priority-based fork detection.
/// Mirrors the legacy `ChainOptions.isPos`.
pub fn is_pos(chain: ChainRef) -> bool {
    matches!(
        chain,
        ChainRef::ChainEthereum
            | ChainRef::ChainGoerli
            | ChainRef::ChainHolesky
            | ChainRef::ChainHoodi
            | ChainRef::ChainSepolia
    )
}
