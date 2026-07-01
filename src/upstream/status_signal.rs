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

//! Per-chain status-change signal.
//!
//! Availability lives in each upstream's
//! [`UpstreamState`](super::state::UpstreamState) as atomics written by several
//! independent tasks (lag tracker, validator, fork watcher, remote-Dshackle
//! status). Consumers that expose the chain's status — the
//! `eth_subscribe("syncing")` egress and the gRPC `SubscribeStatus` — need to
//! react the moment any of those change.
//!
//! [`StatusSignal`] is that wake-up: writers call [`StatusSignal::notify`] after
//! updating their atomic; consumers hold a [`StatusChanges`], await
//! [`StatusChanges::changed`], then recompute the aggregate. It replaces the
//! previous fixed-interval poll, which could miss a status that flipped and
//! recovered inside one interval.

use tokio::sync::watch;

/// A shared wake-up that fires whenever some upstream's availability may have
/// changed. One per chain: held by the chain's `Multistream` and attached to
/// each of its upstreams' [`UpstreamState`](super::state::UpstreamState).
pub struct StatusSignal {
    // An opaque version counter — consumers never read it, they only wait for it
    // to advance. `watch` rather than `Notify` so a change landing between a
    // consumer's recompute and its next `changed()` is still observed, not lost.
    version: watch::Sender<u64>,
}

impl StatusSignal {
    pub fn new() -> Self {
        let (version, _) = watch::channel(0);
        Self { version }
    }

    /// Signal that an upstream's status may have changed. Always wakes waiters,
    /// even when the aggregate turns out unchanged — the consumer re-checks and
    /// suppresses no-op emits itself.
    pub fn notify(&self) {
        self.version.send_modify(|v| *v = v.wrapping_add(1));
    }

    /// A new subscription that wakes on every subsequent [`notify`](Self::notify).
    pub fn subscribe(&self) -> StatusChanges {
        StatusChanges {
            version: self.version.subscribe(),
            _keepalive: None,
        }
    }
}

impl Default for StatusSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// A consumer's handle to a chain's status changes. Await
/// [`changed`](Self::changed) in a loop, recomputing the status each time it
/// returns.
pub struct StatusChanges {
    version: watch::Receiver<u64>,
    // Held only by `never()`, so its `changed()` blocks forever rather than
    // ending the stream: a live sender that never sends.
    _keepalive: Option<watch::Sender<u64>>,
}

impl StatusChanges {
    /// A subscription that never fires — for
    /// [`ChainAccess`](super::egress::ChainAccess) implementors that don't track
    /// upstream status. A consumer emits its initial value and then idles.
    pub fn never() -> Self {
        let (tx, rx) = watch::channel(0);
        Self {
            version: rx,
            _keepalive: Some(tx),
        }
    }

    /// Wait until the chain's status may have changed. Returns `false` when the
    /// signal is gone (the chain was dropped), which should end the consumer's
    /// stream.
    pub async fn changed(&mut self) -> bool {
        self.version.changed().await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn notify_wakes_a_waiter() {
        let signal = StatusSignal::new();
        let mut changes = signal.subscribe();
        signal.notify();
        assert!(changes.changed().await);
    }

    #[tokio::test]
    async fn change_between_checks_is_not_lost() {
        // A notify that lands before the consumer awaits is still delivered —
        // the property the old poll couldn't guarantee.
        let signal = StatusSignal::new();
        let mut changes = signal.subscribe();
        signal.notify();
        signal.notify();
        assert!(changes.changed().await);
    }

    #[tokio::test]
    async fn changed_reports_false_once_signal_dropped() {
        let signal = StatusSignal::new();
        let mut changes = signal.subscribe();
        drop(signal);
        assert!(!changes.changed().await);
    }
}
