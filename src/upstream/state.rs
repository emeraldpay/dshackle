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

//! Shared mutable state for an upstream: lag, validation result, and the
//! derived availability.
//!
//! Two independent writers feed the state: the status tracker recalculates
//! the lag-based status on each tick, and the validator (see
//! [`validation`](super::validation)) reports its probe results. Routing
//! reads the combination of the two.

use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::status_signal::StatusSignal;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};

const NO_LAG: i64 = -1;

/// Thread-safe upstream state updated by the status tracker and the validator.
pub struct UpstreamState {
    lag: AtomicI64,
    /// Availability derived from lag/height by the status tracker.
    lag_status: AtomicU8,
    /// Availability reported by the active validator probes.
    validation_status: AtomicU8,
    /// Availability derived from fork detection: `Immature` while the upstream
    /// is forked off the recognized chain, `Ok` otherwise.
    fork_status: AtomicU8,
    /// Availability reported by the upstream itself over `SubscribeStatus`.
    /// Only a remote Dshackle upstream reports its own status; for every other
    /// upstream this stays `Ok` and has no effect.
    reported_status: AtomicU8,
    /// `disable-validation` in the config: the operator declared this
    /// upstream always valid, so report `Ok` no matter what.
    always_valid: AtomicBool,
    /// Lag threshold above which the upstream is considered syncing.
    /// Ethereum uses 6 (blocks come every ~12s), Bitcoin uses 2 (blocks every ~10min).
    syncing_lag: u64,
    /// The chain's status-change signal, attached when the upstream is grouped
    /// into its `Multistream`. Each mutation wakes it so the chain's status
    /// consumers (`syncing`, gRPC `SubscribeStatus`) react without polling.
    /// Absent for upstreams built standalone or in tests, which mutate silently.
    signal: OnceLock<Arc<StatusSignal>>,
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
            lag_status: AtomicU8::new(UpstreamAvailability::Ok as u8),
            validation_status: AtomicU8::new(UpstreamAvailability::Ok as u8),
            fork_status: AtomicU8::new(UpstreamAvailability::Ok as u8),
            reported_status: AtomicU8::new(UpstreamAvailability::Ok as u8),
            always_valid: AtomicBool::new(false),
            syncing_lag,
            signal: OnceLock::new(),
        }
    }

    /// Attach the chain's status-change signal. Called once, when the upstream
    /// is grouped into its `Multistream`; a second call is ignored. Subsequent
    /// mutations wake the chain's status consumers.
    pub fn attach_status_signal(&self, signal: Arc<StatusSignal>) {
        let _ = self.signal.set(signal);
    }

    /// Wake the chain's status consumers after a mutation. A no-op until a
    /// signal is attached.
    fn notify_change(&self) {
        if let Some(signal) = self.signal.get() {
            signal.notify();
        }
    }

    pub fn lag(&self) -> Option<u64> {
        let v = self.lag.load(Ordering::Relaxed);
        if v < 0 { None } else { Some(v as u64) }
    }

    /// Effective availability: the worst of the lag-based status, the
    /// validation status, the fork status, and the status the upstream reports
    /// about itself (remote Dshackle `SubscribeStatus`).
    ///
    /// The legacy `statusByLag` combined these with special cases that
    /// could let a validation verdict mask a large lag (e.g. `Immature` with
    /// lag 100 stayed `Immature` and kept receiving traffic). Worst-of is
    /// deliberately stricter: each signal alone is enough to take an upstream
    /// out of rotation.
    pub fn availability(&self) -> UpstreamAvailability {
        if self.always_valid.load(Ordering::Relaxed) {
            return UpstreamAvailability::Ok;
        }
        let by_lag = UpstreamAvailability::from_u8(self.lag_status.load(Ordering::Relaxed));
        let by_validation =
            UpstreamAvailability::from_u8(self.validation_status.load(Ordering::Relaxed));
        let by_fork = UpstreamAvailability::from_u8(self.fork_status.load(Ordering::Relaxed));
        let by_reported =
            UpstreamAvailability::from_u8(self.reported_status.load(Ordering::Relaxed));
        by_lag.max(by_validation).max(by_fork).max(by_reported)
    }

    /// Update lag and recalculate the lag-based status.
    ///
    /// `height` is the upstream's own current block height (used to detect
    /// freshly started nodes still at block 0).
    pub fn update(&self, lag: u64, height: Option<u64>) {
        self.lag.store(lag as i64, Ordering::Relaxed);
        let avail = availability_from_lag(lag, height, self.syncing_lag);
        self.lag_status.store(avail as u8, Ordering::Relaxed);
        self.notify_change();
    }

    /// Mark the upstream as having unknown lag (e.g. when head height is not available).
    pub fn set_unknown(&self) {
        self.lag.store(NO_LAG, Ordering::Relaxed);
        self.lag_status
            .store(UpstreamAvailability::Ok as u8, Ordering::Relaxed);
        self.notify_change();
    }

    /// Record the result of a validation round.
    pub fn set_validation(&self, status: UpstreamAvailability) {
        self.validation_status
            .store(status as u8, Ordering::Relaxed);
        self.notify_change();
    }

    /// The last recorded validation result.
    pub fn validation(&self) -> UpstreamAvailability {
        UpstreamAvailability::from_u8(self.validation_status.load(Ordering::Relaxed))
    }

    /// Record a fork-detection verdict. A forked upstream is reported as
    /// `Immature` (matching the legacy fork handling) so it drops out of
    /// rotation; `Ok` clears it.
    pub fn set_fork(&self, forked: bool) {
        let status = if forked {
            UpstreamAvailability::Immature
        } else {
            UpstreamAvailability::Ok
        };
        self.fork_status.store(status as u8, Ordering::Relaxed);
        self.notify_change();
    }

    /// Record the availability a remote Dshackle upstream reports about itself
    /// over `SubscribeStatus`. Folded into `availability` as another worst-of
    /// signal, so the remote declaring a chain unavailable takes it out of
    /// rotation here too.
    pub fn set_reported(&self, status: UpstreamAvailability) {
        self.reported_status.store(status as u8, Ordering::Relaxed);
        self.notify_change();
    }

    /// Declare the upstream always valid (`disable-validation`): availability
    /// is reported as `Ok` regardless of lag or validation results.
    pub fn set_always_valid(&self) {
        self.always_valid.store(true, Ordering::Relaxed);
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

    // ── Validation status ──────────────────────────────────────────────

    #[test]
    fn validation_status_combines_with_lag_as_worst() {
        let s = UpstreamState::new();
        s.update(0, Some(100));
        s.set_validation(UpstreamAvailability::Immature);
        assert_eq!(s.availability(), UpstreamAvailability::Immature);

        // lag grows worse than the validation verdict — lag wins
        s.update(10, Some(100));
        assert_eq!(s.availability(), UpstreamAvailability::Syncing);
    }

    #[test]
    fn unavailable_validation_overrides_healthy_lag() {
        let s = UpstreamState::new();
        s.update(0, Some(100));
        s.set_validation(UpstreamAvailability::Unavailable);
        assert_eq!(s.availability(), UpstreamAvailability::Unavailable);
    }

    #[test]
    fn validation_recovery_restores_lag_status() {
        let s = UpstreamState::new();
        s.update(0, Some(100));
        s.set_validation(UpstreamAvailability::Unavailable);
        s.set_validation(UpstreamAvailability::Ok);
        assert_eq!(s.availability(), UpstreamAvailability::Ok);
    }

    #[test]
    fn set_unknown_preserves_validation_status() {
        let s = UpstreamState::new();
        s.set_validation(UpstreamAvailability::Unavailable);
        s.set_unknown();
        assert_eq!(s.availability(), UpstreamAvailability::Unavailable);
    }

    #[test]
    fn reported_status_combines_as_worst() {
        let s = UpstreamState::new();
        s.update(0, Some(100));
        // A healthy lag, but the remote reports itself unavailable.
        s.set_reported(UpstreamAvailability::Unavailable);
        assert_eq!(s.availability(), UpstreamAvailability::Unavailable);
        // Remote recovers; lag-based Ok is restored.
        s.set_reported(UpstreamAvailability::Ok);
        assert_eq!(s.availability(), UpstreamAvailability::Ok);
    }

    #[test]
    fn always_valid_forces_ok() {
        let s = UpstreamState::new();
        s.set_always_valid();
        s.update(100, Some(100));
        s.set_validation(UpstreamAvailability::Unavailable);
        assert_eq!(s.availability(), UpstreamAvailability::Ok);
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
