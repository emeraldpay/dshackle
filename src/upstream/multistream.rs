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

//! Per-blockchain aggregate of all configured upstreams.
//!
//! `Multistream` is a passive container — it does not route requests itself.
//! Selectors return ordered candidate lists that the
//! [`UpstreamRouter`](super::router) feeds into a `CallQuorum`.
//!
//! The round-robin cursor lives here so that, across requests, different
//! upstreams take the lead position. This spreads the default-strategy load
//! evenly without the router needing to know how many requests have come
//! before.

use crate::jsonrpc::RpcMethod;
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::quorum::{CallQuorum, QuorumFactory, SelectorHint};
use crate::upstream::traits::RpcUpstream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Holds every configured upstream for one blockchain and answers queries
/// about which ones are usable for a given request.
pub struct Multistream {
    upstreams: Vec<Arc<dyn RpcUpstream>>,
    /// Round-robin cursor advanced on each selector call so that successive
    /// requests start from different upstreams.
    cursor: AtomicUsize,
    /// Per-method quorum picker for this chain (Ethereum / Bitcoin / default).
    quorum_factory: Arc<dyn QuorumFactory>,
}

impl Multistream {
    pub fn new(
        upstreams: Vec<Arc<dyn RpcUpstream>>,
        quorum_factory: Arc<dyn QuorumFactory>,
    ) -> Self {
        assert!(!upstreams.is_empty(), "Multistream requires at least one upstream");
        Self {
            upstreams,
            cursor: AtomicUsize::new(0),
            quorum_factory,
        }
    }

    /// Build the `CallQuorum` strategy appropriate for the given RPC method.
    pub fn quorum_for(&self, method: &RpcMethod) -> Box<dyn CallQuorum> {
        self.quorum_factory.quorum_for(method)
    }

    /// Pick candidate upstreams matching the given selector hint.
    pub fn select_for(&self, hint: SelectorHint) -> Vec<Arc<dyn RpcUpstream>> {
        match hint {
            SelectorHint::Available => self.select_available(),
            SelectorHint::NotLagging { max_lag } => self.select_not_lagging(max_lag),
        }
    }

    /// All configured upstreams, in their original (config) order. Used by
    /// the status reporter and for diagnostic snapshots.
    pub fn upstreams(&self) -> &[Arc<dyn RpcUpstream>] {
        &self.upstreams
    }

    /// Total number of upstreams (including currently unavailable ones).
    pub fn len(&self) -> usize {
        self.upstreams.len()
    }

    /// Returns upstreams currently considered usable (`Ok`, `Lagging`, or
    /// `Immature`), starting from the next round-robin position.
    ///
    /// `Syncing` and `Unavailable` upstreams are filtered out — they should
    /// not be tried for a normal call. The starting position is advanced by
    /// one on each call so that load spreads across upstreams over time.
    pub fn select_available(&self) -> Vec<Arc<dyn RpcUpstream>> {
        self.select_where(|u| u.availability() <= UpstreamAvailability::Immature)
    }

    /// Returns available upstreams whose lag is `<= max_lag`. Upstreams with
    /// unknown lag (e.g. one that hasn't reported height yet) are included —
    /// the matching quorum will re-check at record time.
    pub fn select_not_lagging(&self, max_lag: u64) -> Vec<Arc<dyn RpcUpstream>> {
        self.select_where(|u| {
            if u.availability() > UpstreamAvailability::Immature {
                return false;
            }
            match u.lag() {
                Some(l) => l <= max_lag,
                None => true,
            }
        })
    }

    /// Generic selector: returns matching upstreams starting from the next
    /// round-robin position. Used internally by `select_available` and
    /// available for future strategy-specific selectors.
    fn select_where<F>(&self, predicate: F) -> Vec<Arc<dyn RpcUpstream>>
    where
        F: Fn(&Arc<dyn RpcUpstream>) -> bool,
    {
        let n = self.upstreams.len();
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % n;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let u = &self.upstreams[(start + i) % n];
            if predicate(u) {
                out.push(Arc::clone(u));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::methods::DefaultMethods;
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::UpstreamError;
    use std::sync::atomic::AtomicU8;

    struct MockUpstream {
        label: String,
        availability: AtomicU8,
        lag: Option<u64>,
        state: Arc<UpstreamState>,
    }

    impl MockUpstream {
        fn new(label: &str, availability: UpstreamAvailability) -> Arc<Self> {
            Self::with_lag(label, availability, None)
        }

        fn with_lag(
            label: &str,
            availability: UpstreamAvailability,
            lag: Option<u64>,
        ) -> Arc<Self> {
            Arc::new(Self {
                label: label.to_string(),
                availability: AtomicU8::new(availability as u8),
                lag,
                state: Arc::new(UpstreamState::new()),
            })
        }

        fn set_availability(&self, a: UpstreamAvailability) {
            self.availability.store(a as u8, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for MockUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            unimplemented!()
        }
        fn id(&self) -> &str { &self.label }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::from_u8(self.availability.load(Ordering::Relaxed))
        }
        fn head(&self) -> &dyn Head { &NoHead }
        fn lag(&self) -> Option<u64> { self.lag }
        fn state(&self) -> &Arc<UpstreamState> { &self.state }
    }

    fn ms_of(upstreams: Vec<Arc<MockUpstream>>) -> Multistream {
        let dyn_ups: Vec<Arc<dyn RpcUpstream>> =
            upstreams.into_iter().map(|u| u as Arc<dyn RpcUpstream>).collect();
        Multistream::new(dyn_ups, Arc::new(DefaultMethods))
    }

    fn ids(items: &[Arc<dyn RpcUpstream>]) -> Vec<&str> {
        items.iter().map(|u| u.id()).collect()
    }

    #[test]
    fn select_available_returns_all_when_healthy() {
        let ms = ms_of(vec![
            MockUpstream::new("a", UpstreamAvailability::Ok),
            MockUpstream::new("b", UpstreamAvailability::Lagging),
            MockUpstream::new("c", UpstreamAvailability::Immature),
        ]);

        assert_eq!(ms.select_available().len(), 3);
    }

    #[test]
    fn select_available_filters_syncing_and_unavailable() {
        let ms = ms_of(vec![
            MockUpstream::new("a", UpstreamAvailability::Ok),
            MockUpstream::new("b", UpstreamAvailability::Syncing),
            MockUpstream::new("c", UpstreamAvailability::Unavailable),
        ]);

        assert_eq!(ids(&ms.select_available()), vec!["a"]);
    }

    #[test]
    fn select_available_rotates_starting_position() {
        let ms = ms_of(vec![
            MockUpstream::new("a", UpstreamAvailability::Ok),
            MockUpstream::new("b", UpstreamAvailability::Ok),
            MockUpstream::new("c", UpstreamAvailability::Ok),
        ]);

        // Cursor starts at 0 — first call begins at "a".
        assert_eq!(ids(&ms.select_available()), vec!["a", "b", "c"]);
        assert_eq!(ids(&ms.select_available()), vec!["b", "c", "a"]);
        assert_eq!(ids(&ms.select_available()), vec!["c", "a", "b"]);
        assert_eq!(ids(&ms.select_available()), vec!["a", "b", "c"]);
    }

    #[test]
    fn select_available_respects_dynamic_state() {
        let a = MockUpstream::new("a", UpstreamAvailability::Ok);
        let b = MockUpstream::new("b", UpstreamAvailability::Ok);
        let ms = ms_of(vec![a.clone(), b.clone()]);

        a.set_availability(UpstreamAvailability::Unavailable);
        assert_eq!(ids(&ms.select_available()), vec!["b"]);
    }

    #[test]
    fn select_available_returns_empty_when_all_unavailable() {
        let ms = ms_of(vec![
            MockUpstream::new("a", UpstreamAvailability::Unavailable),
            MockUpstream::new("b", UpstreamAvailability::Syncing),
        ]);

        assert!(ms.select_available().is_empty());
    }

    #[test]
    fn select_not_lagging_filters_by_lag() {
        let ms = ms_of(vec![
            MockUpstream::with_lag("fresh", UpstreamAvailability::Ok, Some(0)),
            MockUpstream::with_lag("stale", UpstreamAvailability::Lagging, Some(3)),
            MockUpstream::with_lag("ok", UpstreamAvailability::Lagging, Some(1)),
        ]);

        let picked = ms.select_not_lagging(1);
        let labels: Vec<&str> = ids(&picked);
        assert!(labels.contains(&"fresh"));
        assert!(labels.contains(&"ok"));
        assert!(!labels.contains(&"stale"));
    }

    #[test]
    fn select_not_lagging_keeps_unknown_lag() {
        let ms = ms_of(vec![
            MockUpstream::with_lag("unknown", UpstreamAvailability::Ok, None),
            MockUpstream::with_lag("stale", UpstreamAvailability::Lagging, Some(5)),
        ]);

        let picked = ms.select_not_lagging(0);
        assert_eq!(ids(&picked), vec!["unknown"]);
    }

    #[test]
    fn select_not_lagging_excludes_syncing() {
        let ms = ms_of(vec![
            MockUpstream::with_lag("fresh", UpstreamAvailability::Ok, Some(0)),
            MockUpstream::with_lag("syncing", UpstreamAvailability::Syncing, Some(0)),
        ]);

        let picked = ms.select_not_lagging(0);
        assert_eq!(ids(&picked), vec!["fresh"]);
    }

    #[test]
    fn select_for_dispatches_on_hint() {
        let ms = ms_of(vec![
            MockUpstream::with_lag("a", UpstreamAvailability::Ok, Some(0)),
            MockUpstream::with_lag("b", UpstreamAvailability::Lagging, Some(3)),
        ]);

        assert_eq!(ms.select_for(SelectorHint::Available).len(), 2);
        assert_eq!(
            ids(&ms.select_for(SelectorHint::NotLagging { max_lag: 1 })),
            vec!["a"]
        );
    }

    #[test]
    #[should_panic(expected = "at least one upstream")]
    fn panics_on_empty() {
        let _ms = Multistream::new(vec![], Arc::new(DefaultMethods));
    }
}
