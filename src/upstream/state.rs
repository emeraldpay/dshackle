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

//! Shared mutable state for an upstream: lag and derived availability.
//!
//! Written by the status tracker on each tick, read by `RpcUpstream` trait
//! methods and anything else that needs to know if an upstream is healthy.

use crate::upstream::availability::UpstreamAvailability;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};

const NO_LAG: i64 = -1;

/// Thread-safe upstream state updated by the status tracker.
pub struct UpstreamState {
    lag: AtomicI64,
    availability: AtomicU8,
    /// Lag threshold above which the upstream is considered syncing.
    /// Ethereum uses 6 (blocks come every ~12s), Bitcoin uses 2 (blocks every ~10min).
    syncing_lag: u64,
}

impl UpstreamState {
    /// Creates a new state with the default syncing threshold of 6 (suitable for Ethereum).
    pub fn new() -> Self {
        Self::with_syncing_lag(6)
    }

    /// Creates a new state with a custom syncing lag threshold.
    ///
    /// When the upstream's lag exceeds this value it is marked as `Syncing`.
    pub fn with_syncing_lag(syncing_lag: u64) -> Self {
        Self {
            lag: AtomicI64::new(NO_LAG),
            availability: AtomicU8::new(UpstreamAvailability::Ok as u8),
            syncing_lag,
        }
    }

    pub fn lag(&self) -> Option<u64> {
        let v = self.lag.load(Ordering::Relaxed);
        if v < 0 { None } else { Some(v as u64) }
    }

    pub fn availability(&self) -> UpstreamAvailability {
        UpstreamAvailability::from_u8(self.availability.load(Ordering::Relaxed))
    }

    /// Update lag and recalculate availability based on the legacy `statusByLag` rules.
    ///
    /// `height` is the upstream's own current block height (used to detect
    /// freshly started nodes still at block 0).
    pub fn update(&self, lag: u64, height: Option<u64>) {
        self.lag.store(lag as i64, Ordering::Relaxed);
        let avail = availability_from_lag(lag, height, self.syncing_lag);
        self.availability.store(avail as u8, Ordering::Relaxed);
    }

    /// Mark the upstream as having unknown lag (e.g. when head height is not available).
    pub fn set_unknown(&self) {
        self.lag.store(NO_LAG, Ordering::Relaxed);
        self.availability.store(UpstreamAvailability::Ok as u8, Ordering::Relaxed);
    }
}

/// Derives availability status from lag and height, matching the legacy
/// `DefaultUpstream.statusByLag` / `getStatus` logic.
///
/// `syncing_lag` is the threshold above which the upstream is marked as syncing
/// (6 for Ethereum, 2 for Bitcoin).
fn availability_from_lag(lag: u64, height: Option<u64>, syncing_lag: u64) -> UpstreamAvailability {
    if height == Some(0) {
        return UpstreamAvailability::Syncing;
    }
    if lag > syncing_lag {
        UpstreamAvailability::Syncing
    } else if lag > 1 {
        UpstreamAvailability::Lagging
    } else {
        UpstreamAvailability::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Default (Ethereum) threshold of 6 ──────────────────────────────

    #[test]
    fn starts_with_ok_and_no_lag() {
        let s = UpstreamState::new();
        assert_eq!(s.availability(), UpstreamAvailability::Ok);
        assert_eq!(s.lag(), None);
    }

    #[test]
    fn zero_lag_is_ok() {
        let s = UpstreamState::new();
        s.update(0, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Ok);
        assert_eq!(s.lag(), Some(0));
    }

    #[test]
    fn lag_1_is_ok() {
        let s = UpstreamState::new();
        s.update(1, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Ok);
        assert_eq!(s.lag(), Some(1));
    }

    #[test]
    fn lag_2_is_lagging() {
        let s = UpstreamState::new();
        s.update(2, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Lagging);
    }

    #[test]
    fn lag_6_is_lagging() {
        let s = UpstreamState::new();
        s.update(6, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Lagging);
    }

    #[test]
    fn lag_7_is_syncing() {
        let s = UpstreamState::new();
        s.update(7, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Syncing);
    }

    #[test]
    fn height_zero_is_syncing_regardless_of_lag() {
        let s = UpstreamState::new();
        s.update(0, Some(0));
        assert_eq!(s.availability(), UpstreamAvailability::Syncing);
    }

    #[test]
    fn unknown_height_uses_lag_rules() {
        let s = UpstreamState::new();
        s.update(3, None);
        assert_eq!(s.availability(), UpstreamAvailability::Lagging);
    }

    #[test]
    fn set_unknown_resets_to_ok_no_lag() {
        let s = UpstreamState::new();
        s.update(10, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Syncing);
        s.set_unknown();
        assert_eq!(s.availability(), UpstreamAvailability::Ok);
        assert_eq!(s.lag(), None);
    }

    // ── Bitcoin threshold of 2 ─────────────────────────────────────────

    #[test]
    fn bitcoin_lag_2_is_lagging() {
        let s = UpstreamState::with_syncing_lag(2);
        s.update(2, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Lagging);
    }

    #[test]
    fn bitcoin_lag_3_is_syncing() {
        let s = UpstreamState::with_syncing_lag(2);
        s.update(3, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Syncing);
    }

    #[test]
    fn bitcoin_lag_1_is_ok() {
        let s = UpstreamState::with_syncing_lag(2);
        s.update(1, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Ok);
    }
}
