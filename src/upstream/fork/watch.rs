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

//! The fork watcher: feeds each upstream's blocks to its chain's fork choice.

use super::ForkChoice;
use crate::upstream::head::CurrentHead;
use crate::upstream::state::UpstreamState;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

/// An upstream as the fork wiring sees it: its id and priority, the head to
/// watch, and the state to record the verdict on.
pub struct ForkMember {
    pub id: String,
    pub priority: i32,
    pub head: Arc<CurrentHead>,
    pub state: Arc<UpstreamState>,
}

/// Spawn a task that submits each new block from `head` to the shared
/// `fork_choice` and records the verdict on the upstream's state, taking a
/// forked upstream out of rotation.
///
/// Unlike the legacy `ForkWatch` — which shared one watcher per chain and only
/// followed the last-registered upstream — every upstream gets its own watcher
/// here, so every upstream's blocks reach the shared fork choice.
pub fn start_fork_watch(
    id: String,
    head: Arc<CurrentHead>,
    state: Arc<UpstreamState>,
    fork_choice: Arc<dyn ForkChoice>,
) {
    tokio::spawn(async move {
        let mut blocks = head.subscribe_blocks();
        loop {
            match blocks.recv().await {
                Ok(block) => {
                    let status = fork_choice.submit(&block, &id);
                    state.set_fork(!status.is_ok());
                    if !status.is_ok() {
                        tracing::warn!(
                            upstream = %id,
                            choice = fork_choice.name(),
                            "Upstream is forked from the recognized chain"
                        );
                    }
                }
                // The head stream keeps only the latest blocks; missing a few
                // is fine since only the current head matters.
                Err(RecvError::Lagged(skipped)) => {
                    tracing::debug!(upstream = %id, skipped, "Fork watch lagged behind head stream");
                }
                Err(RecvError::Closed) => break,
            }
        }
    });
}
