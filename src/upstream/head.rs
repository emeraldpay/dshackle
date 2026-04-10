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

//! Head tracking for blockchain upstreams.
//!
//! A `Head` represents the current tip of the chain as seen by an upstream.
//! Future iterations will add block streaming (like the legacy `getFlux()`)
//! and pre-block hooks.

use std::sync::atomic::{AtomicI64, Ordering};

/// Sentinel value indicating "no height known yet" (stored in the atomic).
const NO_HEIGHT: i64 = -1;

/// Current chain tip as observed by an upstream.
pub trait Head: Send + Sync {
    /// Returns the latest known block height, or `None` if not yet available.
    fn current_height(&self) -> Option<u64>;
}

/// A `Head` that has no data yet — used as a placeholder until real head
/// tracking is wired up.
pub struct NoHead;

impl Head for NoHead {
    fn current_height(&self) -> Option<u64> {
        None
    }
}

/// Thread-safe, atomically updated head height.
///
/// Shared between the background polling task (writer) and the status
/// reporter / trait impl (readers).
pub struct CurrentHeight {
    height: AtomicI64,
}

impl CurrentHeight {
    pub fn new() -> Self {
        Self {
            height: AtomicI64::new(NO_HEIGHT),
        }
    }

    /// Update the tracked height. Only accepts forward progress — a lower
    /// height is silently ignored to avoid temporary dips from stale responses.
    pub fn update(&self, new_height: u64) {
        self.height.fetch_max(new_height as i64, Ordering::Relaxed);
    }
}

impl Head for CurrentHeight {
    fn current_height(&self) -> Option<u64> {
        let h = self.height.load(Ordering::Relaxed);
        if h < 0 {
            None
        } else {
            Some(h as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_with_no_height() {
        let h = CurrentHeight::new();
        assert_eq!(h.current_height(), None);
    }

    #[test]
    fn tracks_height_updates() {
        let h = CurrentHeight::new();
        h.update(100);
        assert_eq!(h.current_height(), Some(100));
        h.update(200);
        assert_eq!(h.current_height(), Some(200));
    }

    #[test]
    fn ignores_lower_height() {
        let h = CurrentHeight::new();
        h.update(200);
        h.update(100);
        assert_eq!(h.current_height(), Some(200));
    }
}
