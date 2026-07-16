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

use crate::blockchain::TargetBlockchain;
use crate::config::upstreams::UpstreamRole;
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::egress::ChainAccess;
use crate::upstream::quorum::{CallQuorum, QuorumFactory, SelectorHint};
use crate::upstream::router;
use crate::upstream::status_signal::{StatusChanges, StatusSignal};
use crate::upstream::traits::{Capability, RpcUpstream, UpstreamError};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Holds every configured upstream for one blockchain and answers queries
/// about which ones are usable for a given request.
pub struct Multistream {
    chain: TargetBlockchain,
    upstreams: Vec<Arc<dyn RpcUpstream>>,
    /// Upstreams grouped by routing role — primary, secondary, fallback —
    /// preserving config order within each tier. Roles are fixed at wiring
    /// time, so the partition is computed once here instead of on every
    /// selection call.
    tiers: [Vec<Arc<dyn RpcUpstream>>; 3],
    /// Round-robin cursor advanced on each selector call so that successive
    /// requests start from different upstreams.
    cursor: AtomicUsize,
    /// Per-method quorum picker for this chain (Ethereum / Bitcoin / default).
    quorum_factory: Arc<dyn QuorumFactory>,
    /// Wakes on every upstream status change, shared with each upstream's state.
    /// Drives the event-driven `syncing` egress and gRPC `SubscribeStatus`.
    status_signal: Arc<StatusSignal>,
}

impl Multistream {
    pub fn new(
        chain: TargetBlockchain,
        upstreams: Vec<Arc<dyn RpcUpstream>>,
        quorum_factory: Arc<dyn QuorumFactory>,
    ) -> Self {
        assert!(
            !upstreams.is_empty(),
            "Multistream requires at least one upstream"
        );
        // One signal per chain, shared into every upstream's state so any status
        // writer (lag tracker, validator, fork watcher, remote status) wakes the
        // chain's status consumers.
        let status_signal = Arc::new(StatusSignal::new());
        for upstream in &upstreams {
            upstream
                .state()
                .attach_status_signal(Arc::clone(&status_signal));
        }
        let mut tiers: [Vec<Arc<dyn RpcUpstream>>; 3] = Default::default();
        for upstream in &upstreams {
            let tier = match upstream.role() {
                UpstreamRole::Primary => 0,
                UpstreamRole::Secondary => 1,
                UpstreamRole::Fallback => 2,
            };
            tiers[tier].push(Arc::clone(upstream));
        }
        Self {
            chain,
            upstreams,
            tiers,
            cursor: AtomicUsize::new(0),
            quorum_factory,
            status_signal,
        }
    }

    /// The blockchain all these upstreams serve.
    pub fn chain(&self) -> &TargetBlockchain {
        &self.chain
    }

    /// A subscription that wakes whenever some upstream's availability may have
    /// changed. Backs the event-driven `syncing` egress and gRPC
    /// `SubscribeStatus`, replacing their former fixed-interval polls.
    pub fn status_changes(&self) -> StatusChanges {
        self.status_signal.subscribe()
    }

    /// Build the `CallQuorum` strategy appropriate for the given RPC method.
    pub fn quorum_for(&self, method: &RpcMethod) -> Box<dyn CallQuorum> {
        self.quorum_factory.quorum_for(method)
    }

    /// Every method this chain can serve, for `Describe`. Sorted, callable plus
    /// hardcoded, aggregated across all upstreams' method configs.
    pub fn supported_methods(&self) -> Vec<String> {
        self.quorum_factory.supported_methods()
    }

    /// Pick candidate upstreams matching the given selector hint, filtered to
    /// those that accept `method`. Upstreams that reject the method up front
    /// are skipped so the router doesn't waste a round-trip just to learn
    /// what the allow-list already knew.
    pub fn select_for(&self, hint: SelectorHint, method: &RpcMethod) -> Vec<Arc<dyn RpcUpstream>> {
        match hint {
            SelectorHint::Available => self.select_available(method),
            SelectorHint::NotLagging { max_lag } => self.select_not_lagging(method, max_lag),
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

    /// The chain's overall availability: the best (most-available) status across
    /// all upstreams. Upstreams are never empty (asserted in `new`).
    pub fn aggregate_availability(&self) -> UpstreamAvailability {
        self.upstreams
            .iter()
            .map(|u| u.availability())
            .min()
            .unwrap_or(UpstreamAvailability::Unavailable)
    }

    /// Returns upstreams currently considered usable (`Ok`, `Lagging`, or
    /// `Immature`) that accept `method`, starting from the next round-robin
    /// position.
    ///
    /// `Syncing` and `Unavailable` upstreams are filtered out — they should
    /// not be tried for a normal call. The starting position is advanced by
    /// one on each call so that load spreads across upstreams over time.
    pub fn select_available(&self, method: &RpcMethod) -> Vec<Arc<dyn RpcUpstream>> {
        self.select_where(|u| {
            serves_rpc(u)
                && u.availability() <= UpstreamAvailability::Immature
                && u.allows_method(method)
        })
    }

    /// Returns available upstreams that accept `method` and whose lag is
    /// `<= max_lag`. Upstreams with unknown lag (e.g. one that hasn't reported
    /// height yet) are included — the matching quorum will re-check at record
    /// time.
    pub fn select_not_lagging(
        &self,
        method: &RpcMethod,
        max_lag: u64,
    ) -> Vec<Arc<dyn RpcUpstream>> {
        self.select_where(|u| {
            if !serves_rpc(u) {
                return false;
            }
            if u.availability() > UpstreamAvailability::Immature {
                return false;
            }
            if !u.allows_method(method) {
                return false;
            }
            match u.lag() {
                Some(l) => l <= max_lag,
                None => true,
            }
        })
    }

    /// Generic selector: returns matching upstreams grouped by role tier
    /// (primary, then secondary, then fallback), each tier rotated by the
    /// shared round-robin cursor.
    ///
    /// Mirrors the legacy `FilteredApis` ordering: a `Fallback` upstream is
    /// only reached after every primary and secondary candidate failed, and
    /// same-tier load still spreads round-robin. Unlike legacy there are no
    /// delayed retry cycles — the router walks the list once, so fallbacks
    /// are simply appended at the tail.
    fn select_where<F>(&self, predicate: F) -> Vec<Arc<dyn RpcUpstream>>
    where
        F: Fn(&Arc<dyn RpcUpstream>) -> bool,
    {
        crate::metrics::select_exist(
            &self.chain,
            self.tiers[0].len(),
            self.tiers[1].len(),
            self.tiers[2].len(),
        );
        let pos = self.cursor.fetch_add(1, Ordering::Relaxed);
        let mut out = Vec::with_capacity(self.upstreams.len());
        for tier in &self.tiers {
            if tier.is_empty() {
                continue;
            }
            let start = pos % tier.len();
            for i in 0..tier.len() {
                let u = &tier[(start + i) % tier.len()];
                if predicate(u) {
                    out.push(Arc::clone(u));
                }
            }
        }
        out
    }
}

/// Whether an upstream may serve RPC (`NativeCall`) requests. Mirrors the legacy
/// `Selector.CapabilityMatcher(Capability.RPC)`: a remote Dshackle advertising
/// only `BALANCE` (and not `CALLS`) must be kept out of call routing even though
/// it may list callable methods. Local upstreams always advertise `Rpc`.
fn serves_rpc(u: &Arc<dyn RpcUpstream>) -> bool {
    u.capabilities().contains(&Capability::Rpc)
}

/// Keep only candidates whose head has reached `min_height`, following the
/// legacy `HeightMatcher`: an upstream that never reported a head counts as
/// height 0.
///
/// When *no* candidate reports a height at all — the startup window before
/// the first head poll — the list is returned unchanged. There is nothing to
/// compare against yet, and failing every block-pinned read on each deploy
/// would trade a possible stale answer for a guaranteed error (legacy masked
/// the same window with its delayed retry cycles).
pub fn at_height(
    mut candidates: Vec<Arc<dyn RpcUpstream>>,
    min_height: u64,
) -> Vec<Arc<dyn RpcUpstream>> {
    let any_known = candidates
        .iter()
        .any(|u| u.head().current_height().is_some());
    if any_known {
        candidates.retain(|u| u.head().current_height().unwrap_or(0) >= min_height);
    }
    candidates
}

#[async_trait::async_trait]
impl ChainAccess for Multistream {
    fn is_syncing(&self) -> bool {
        self.aggregate_availability() != UpstreamAvailability::Ok
    }

    fn status_changes(&self) -> StatusChanges {
        Multistream::status_changes(self)
    }

    fn current_height(&self) -> Option<u64> {
        // An unavailable or syncing upstream's height cannot be trusted — a
        // node that failed validation may report a bogus head, and using it
        // as the chain's height would filter every healthy upstream out of
        // height-constrained routing.
        self.upstreams
            .iter()
            .filter(|u| u.availability() <= UpstreamAvailability::Immature)
            .filter_map(|u| u.head().current_height())
            .max()
    }

    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        // The same routing core as `native_call::execute_call`, inlined to keep
        // the upstream layer from depending on the rpc layer.
        let quorum = self.quorum_for(&request.method);
        let candidates = self.select_for(quorum.selector(), &request.method);
        router::route(&self.chain, candidates, quorum, request)
            .await
            .map(|routed| routed.response)
    }

    async fn call_at_height(
        &self,
        request: &JsonRpcRequest,
        min_height: u64,
    ) -> Result<JsonRpcResponse, UpstreamError> {
        let quorum = self.quorum_for(&request.method);
        let candidates = self.select_for(quorum.selector(), &request.method);
        // Keep only upstreams that have actually reached the block; a plain
        // `call` may pick one lagging within the method's tolerance, which
        // answers the read with `null`.
        let filtered = at_height(candidates.clone(), min_height);
        // Fall back to the unfiltered set when no upstream reports a head at or
        // above the block — best effort, the block may genuinely be unavailable.
        // (Client-facing routing in `execute_call` instead fails hard here.)
        let candidates = if filtered.is_empty() {
            candidates
        } else {
            filtered
        };
        router::route(&self.chain, candidates, quorum, request)
            .await
            .map(|routed| routed.response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::head::{CurrentHead, Head, NoHead};
    use crate::upstream::id::UpstreamId;
    use crate::upstream::methods::DefaultMethods;
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::UpstreamError;
    use std::sync::atomic::AtomicU8;

    struct MockUpstream {
        label: UpstreamId,
        availability: AtomicU8,
        lag: Option<u64>,
        state: Arc<UpstreamState>,
        head: Arc<CurrentHead>,
        role: UpstreamRole,
        /// Canned result served for any call, when set.
        result: Option<serde_json::Value>,
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
                label: label.parse().unwrap(),
                availability: AtomicU8::new(availability as u8),
                lag,
                state: Arc::new(UpstreamState::new()),
                head: Arc::new(CurrentHead::new()),
                role: UpstreamRole::Primary,
                result: None,
            })
        }

        fn with_role(label: &str, role: UpstreamRole) -> Arc<Self> {
            Arc::new(Self {
                label: label.parse().unwrap(),
                availability: AtomicU8::new(UpstreamAvailability::Ok as u8),
                lag: Some(0),
                state: Arc::new(UpstreamState::new()),
                head: Arc::new(CurrentHead::new()),
                role,
                result: None,
            })
        }

        /// A healthy mock whose head is at `height` and which serves `result`
        /// for any call — used to exercise height-aware routing.
        fn serving(label: &str, height: u64, result: serde_json::Value) -> Arc<Self> {
            let head = CurrentHead::new();
            head.update(height);
            Arc::new(Self {
                label: label.parse().unwrap(),
                availability: AtomicU8::new(UpstreamAvailability::Ok as u8),
                lag: Some(0),
                state: Arc::new(UpstreamState::new()),
                head: Arc::new(head),
                role: UpstreamRole::Primary,
                result: Some(result),
            })
        }

        fn set_availability(&self, a: UpstreamAvailability) {
            self.availability.store(a as u8, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for MockUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            match &self.result {
                Some(value) => {
                    let body = format!(
                        r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                        serde_json::to_string(value).unwrap()
                    );
                    Ok(serde_json::from_str(&body).unwrap())
                }
                None => unimplemented!(),
            }
        }
        fn id(&self) -> &UpstreamId {
            &self.label
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::from_u8(self.availability.load(Ordering::Relaxed))
        }
        fn head(&self) -> &dyn Head {
            self.head.as_ref()
        }
        fn lag(&self) -> Option<u64> {
            self.lag
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
        fn role(&self) -> UpstreamRole {
            self.role
        }
    }

    fn test_chain() -> TargetBlockchain {
        TargetBlockchain::Standard(emerald_api::proto::common::ChainRef::ChainEthereum)
    }

    fn ms_of(upstreams: Vec<Arc<MockUpstream>>) -> Multistream {
        let dyn_ups: Vec<Arc<dyn RpcUpstream>> = upstreams
            .into_iter()
            .map(|u| u as Arc<dyn RpcUpstream>)
            .collect();
        Multistream::new(test_chain(), dyn_ups, Arc::new(DefaultMethods))
    }

    fn ids(items: &[Arc<dyn RpcUpstream>]) -> Vec<&str> {
        items.iter().map(|u| u.id().as_str()).collect()
    }

    #[test]
    fn select_available_returns_all_when_healthy() {
        let ms = ms_of(vec![
            MockUpstream::new("up-a", UpstreamAvailability::Ok),
            MockUpstream::new("up-b", UpstreamAvailability::Lagging),
            MockUpstream::new("up-c", UpstreamAvailability::Immature),
        ]);

        assert_eq!(ms.select_available(&"any".into()).len(), 3);
    }

    #[test]
    fn select_available_filters_syncing_and_unavailable() {
        let ms = ms_of(vec![
            MockUpstream::new("up-a", UpstreamAvailability::Ok),
            MockUpstream::new("up-b", UpstreamAvailability::Syncing),
            MockUpstream::new("up-c", UpstreamAvailability::Unavailable),
        ]);

        assert_eq!(ids(&ms.select_available(&"any".into())), vec!["up-a"]);
    }

    #[test]
    fn select_available_rotates_starting_position() {
        let ms = ms_of(vec![
            MockUpstream::new("up-a", UpstreamAvailability::Ok),
            MockUpstream::new("up-b", UpstreamAvailability::Ok),
            MockUpstream::new("up-c", UpstreamAvailability::Ok),
        ]);

        // Cursor starts at 0 — first call begins at "up-a".
        assert_eq!(
            ids(&ms.select_available(&"any".into())),
            vec!["up-a", "up-b", "up-c"]
        );
        assert_eq!(
            ids(&ms.select_available(&"any".into())),
            vec!["up-b", "up-c", "up-a"]
        );
        assert_eq!(
            ids(&ms.select_available(&"any".into())),
            vec!["up-c", "up-a", "up-b"]
        );
        assert_eq!(
            ids(&ms.select_available(&"any".into())),
            vec!["up-a", "up-b", "up-c"]
        );
    }

    #[test]
    fn select_available_respects_dynamic_state() {
        let a = MockUpstream::new("up-a", UpstreamAvailability::Ok);
        let b = MockUpstream::new("up-b", UpstreamAvailability::Ok);
        let ms = ms_of(vec![a.clone(), b.clone()]);

        a.set_availability(UpstreamAvailability::Unavailable);
        assert_eq!(ids(&ms.select_available(&"any".into())), vec!["up-b"]);
    }

    #[test]
    fn select_available_returns_empty_when_all_unavailable() {
        let ms = ms_of(vec![
            MockUpstream::new("up-a", UpstreamAvailability::Unavailable),
            MockUpstream::new("up-b", UpstreamAvailability::Syncing),
        ]);

        assert!(ms.select_available(&"any".into()).is_empty());
    }

    #[test]
    fn select_not_lagging_filters_by_lag() {
        let ms = ms_of(vec![
            MockUpstream::with_lag("fresh", UpstreamAvailability::Ok, Some(0)),
            MockUpstream::with_lag("stale", UpstreamAvailability::Lagging, Some(3)),
            MockUpstream::with_lag("up-ok", UpstreamAvailability::Lagging, Some(1)),
        ]);

        let picked = ms.select_not_lagging(&"any".into(), 1);
        let labels: Vec<&str> = ids(&picked);
        assert!(labels.contains(&"fresh"));
        assert!(labels.contains(&"up-ok"));
        assert!(!labels.contains(&"stale"));
    }

    #[test]
    fn select_not_lagging_keeps_unknown_lag() {
        let ms = ms_of(vec![
            MockUpstream::with_lag("unknown", UpstreamAvailability::Ok, None),
            MockUpstream::with_lag("stale", UpstreamAvailability::Lagging, Some(5)),
        ]);

        let picked = ms.select_not_lagging(&"any".into(), 0);
        assert_eq!(ids(&picked), vec!["unknown"]);
    }

    #[test]
    fn select_not_lagging_excludes_syncing() {
        let ms = ms_of(vec![
            MockUpstream::with_lag("fresh", UpstreamAvailability::Ok, Some(0)),
            MockUpstream::with_lag("syncing", UpstreamAvailability::Syncing, Some(0)),
        ]);

        let picked = ms.select_not_lagging(&"any".into(), 0);
        assert_eq!(ids(&picked), vec!["fresh"]);
    }

    #[test]
    fn select_for_dispatches_on_hint() {
        let ms = ms_of(vec![
            MockUpstream::with_lag("up-a", UpstreamAvailability::Ok, Some(0)),
            MockUpstream::with_lag("up-b", UpstreamAvailability::Lagging, Some(3)),
        ]);

        assert_eq!(
            ms.select_for(SelectorHint::Available, &"any".into()).len(),
            2
        );
        assert_eq!(
            ids(&ms.select_for(SelectorHint::NotLagging { max_lag: 1 }, &"any".into())),
            vec!["up-a"]
        );
    }

    #[test]
    fn fallback_selected_after_primary_regardless_of_config_order() {
        let ms = ms_of(vec![
            MockUpstream::with_role("backup", UpstreamRole::Fallback),
            MockUpstream::with_role("main", UpstreamRole::Primary),
        ]);

        assert_eq!(
            ids(&ms.select_available(&"any".into())),
            vec!["main", "backup"]
        );
    }

    #[test]
    fn tiers_ordered_primary_secondary_fallback() {
        let ms = ms_of(vec![
            MockUpstream::with_role("up-f", UpstreamRole::Fallback),
            MockUpstream::with_role("up-s", UpstreamRole::Secondary),
            MockUpstream::with_role("up-p", UpstreamRole::Primary),
        ]);

        assert_eq!(
            ids(&ms.select_available(&"any".into())),
            vec!["up-p", "up-s", "up-f"]
        );
    }

    #[test]
    fn rotation_stays_within_tier() {
        let ms = ms_of(vec![
            MockUpstream::with_role("up-a", UpstreamRole::Primary),
            MockUpstream::with_role("up-b", UpstreamRole::Primary),
            MockUpstream::with_role("up-f", UpstreamRole::Fallback),
        ]);

        // The round-robin cursor rotates primaries between calls, but the
        // fallback never leaves the tail position.
        assert_eq!(
            ids(&ms.select_available(&"any".into())),
            vec!["up-a", "up-b", "up-f"]
        );
        assert_eq!(
            ids(&ms.select_available(&"any".into())),
            vec!["up-b", "up-a", "up-f"]
        );
        assert_eq!(
            ids(&ms.select_available(&"any".into())),
            vec!["up-a", "up-b", "up-f"]
        );
    }

    #[test]
    fn current_height_ignores_unavailable_upstreams() {
        // A sick upstream may report a bogus (or stale-frozen) head; trusting
        // it would filter every healthy upstream out of "latest" routing.
        let sick = MockUpstream::serving("sick", 1005, serde_json::Value::Null);
        let healthy = MockUpstream::serving("healthy", 1004, serde_json::Value::Null);
        let ms = ms_of(vec![sick.clone(), healthy]);

        assert_eq!(ChainAccess::current_height(&ms), Some(1005));
        sick.set_availability(UpstreamAvailability::Unavailable);
        assert_eq!(ChainAccess::current_height(&ms), Some(1004));
    }

    #[test]
    fn at_height_keeps_only_upstreams_at_the_block() {
        let pruned = MockUpstream::serving("pruned", 50, serde_json::Value::Null);
        let full = MockUpstream::serving("full", 200, serde_json::Value::Null);
        let candidates: Vec<Arc<dyn RpcUpstream>> = vec![pruned, full];

        let kept = at_height(candidates, 100);
        assert_eq!(ids(&kept), vec!["full"]);
    }

    #[test]
    fn at_height_excludes_unknown_head_when_others_are_known() {
        // Legacy `HeightMatcher`: an unreported head counts as height 0.
        let unknown = MockUpstream::new("unknown", UpstreamAvailability::Ok);
        let known = MockUpstream::serving("known", 200, serde_json::Value::Null);
        let candidates: Vec<Arc<dyn RpcUpstream>> = vec![unknown, known];

        let kept = at_height(candidates, 100);
        assert_eq!(ids(&kept), vec!["known"]);
    }

    #[test]
    fn at_height_passes_all_through_when_no_head_is_known_yet() {
        // The startup window before the first head poll: with nothing to
        // compare against, filtering would fail every pinned read on deploy.
        let a = MockUpstream::new("up-a", UpstreamAvailability::Ok);
        let b = MockUpstream::new("up-b", UpstreamAvailability::Ok);
        let candidates: Vec<Arc<dyn RpcUpstream>> = vec![a, b];

        let kept = at_height(candidates, 100);
        assert_eq!(kept.len(), 2);
    }

    #[test]
    #[should_panic(expected = "at least one upstream")]
    fn panics_on_empty() {
        let _ms = Multistream::new(test_chain(), vec![], Arc::new(DefaultMethods));
    }

    #[tokio::test]
    async fn call_at_height_reads_from_an_upstream_that_has_the_block() {
        // `lagging` is first in round-robin order and would answer a height-100
        // read with null; only `tip` actually has block 100. `call_at_height`
        // must skip `lagging` and read from `tip`.
        let lagging = MockUpstream::serving("lagging", 99, serde_json::Value::Null);
        let tip = MockUpstream::serving("tip", 100, serde_json::json!({ "number": "0x64" }));
        let ms = ms_of(vec![lagging, tip]);

        let req = JsonRpcRequest::new(
            0,
            "eth_getBlockByNumber".into(),
            serde_json::json!(["0x64", true]),
        );
        let resp = ms.call_at_height(&req, 100).await.unwrap();
        let result: serde_json::Value = serde_json::from_str(resp.result.unwrap().get()).unwrap();
        assert_eq!(result["number"], "0x64");
    }

    #[tokio::test]
    async fn call_at_height_falls_back_when_no_upstream_reports_the_block() {
        // No upstream has reached height 200; rather than routing to nobody,
        // it best-efforts the read against the available upstream.
        let a = MockUpstream::serving("up-a", 100, serde_json::json!({ "number": "0x64" }));
        let ms = ms_of(vec![a]);

        let req = JsonRpcRequest::new(
            0,
            "eth_getBlockByNumber".into(),
            serde_json::json!(["0xc8", true]),
        );
        let resp = ms.call_at_height(&req, 200).await.unwrap();
        let result: serde_json::Value = serde_json::from_str(resp.result.unwrap().get()).unwrap();
        assert_eq!(result["number"], "0x64");
    }

    /// Mock upstream with a hardcoded allow-list, used to verify that the
    /// selector skips upstreams that don't support the requested method.
    struct MethodGatedUpstream {
        label: UpstreamId,
        allows: Vec<String>,
        state: Arc<UpstreamState>,
    }

    #[async_trait::async_trait]
    impl RpcUpstream for MethodGatedUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            unimplemented!()
        }
        fn id(&self) -> &UpstreamId {
            &self.label
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::Ok
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            Some(0)
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
        fn allows_method(&self, method: &RpcMethod) -> bool {
            self.allows.iter().any(|m| m == method.as_str())
        }
    }

    fn gated(label: &str, allows: &[&str]) -> Arc<MethodGatedUpstream> {
        Arc::new(MethodGatedUpstream {
            label: label.parse().unwrap(),
            allows: allows.iter().map(|s| s.to_string()).collect(),
            state: Arc::new(UpstreamState::new()),
        })
    }

    #[test]
    fn select_skips_upstreams_that_reject_method() {
        let dyn_ups: Vec<Arc<dyn RpcUpstream>> = vec![
            gated("up-a", &["eth_getBalance"]) as Arc<dyn RpcUpstream>,
            gated("up-b", &["debug_traceTransaction"]) as Arc<dyn RpcUpstream>,
            gated("up-c", &["eth_getBalance", "debug_traceTransaction"]) as Arc<dyn RpcUpstream>,
        ];
        let ms = Multistream::new(test_chain(), dyn_ups, Arc::new(DefaultMethods));

        // `b` doesn't support eth_getBalance — it must not appear.
        let picked = ms.select_available(&"eth_getBalance".into());
        let labels = ids(&picked);
        assert!(labels.contains(&"up-a"));
        assert!(!labels.contains(&"up-b"));
        assert!(labels.contains(&"up-c"));
    }

    #[test]
    fn select_not_lagging_also_honours_method_filter() {
        let dyn_ups: Vec<Arc<dyn RpcUpstream>> = vec![
            gated("archive", &["debug_traceTransaction"]) as Arc<dyn RpcUpstream>,
            gated("basic", &["eth_getBalance"]) as Arc<dyn RpcUpstream>,
        ];
        let ms = Multistream::new(test_chain(), dyn_ups, Arc::new(DefaultMethods));

        let picked = ms.select_not_lagging(&"debug_traceTransaction".into(), 0);
        assert_eq!(ids(&picked), vec!["archive"]);
    }
}
