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
//! [`Multistream::execute`] is where a call is routed: it's prepared by the
//! chain's [`CallPlanner`], the upstreams that can answer the plan are
//! selected, and the [`UpstreamRouter`](super::router) runs them through the
//! method's `CallQuorum`.
//!
//! The round-robin cursor lives here so that, across requests, different
//! upstreams take the lead position. This spreads the default-strategy load
//! evenly without the router needing to know how many requests have come
//! before.

use crate::blockchain::TargetBlockchain;
use crate::config::upstreams::UpstreamRole;
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::call_plan::{AsIs, BlockRead, CallPlanner};
use crate::upstream::egress::ChainAccess;
use crate::upstream::quorum::{CallQuorum, QuorumFactory, SelectorHint};
use crate::upstream::router::{self, Routed};
use crate::upstream::selector::LabelSelector;
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
    /// Prepares each call before routing (see [`call_plan`](super::call_plan)).
    planner: Arc<dyn CallPlanner>,
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
            planner: Arc::new(AsIs),
            status_signal,
        }
    }

    /// Use the chain's own rules to prepare calls (see [`CallPlanner`]);
    /// without it they're routed as the client sent them.
    pub fn with_planner(mut self, planner: Arc<dyn CallPlanner>) -> Self {
        self.planner = planner;
        self
    }

    /// Route a call: prepare it, select the upstreams that can answer it, and
    /// run it through its method's quorum. Candidates must also pass the
    /// client's label selector.
    pub async fn execute(
        &self,
        request: &JsonRpcRequest,
        labels: &LabelSelector,
    ) -> Result<Routed, UpstreamError> {
        self.execute_preferring(request, labels, None).await
    }

    /// [`execute`](Self::execute), preferring upstreams whose head has reached
    /// `height` when there are any: an internal read of a block it knows
    /// exists (e.g. for fee estimation) shouldn't land on a node that answers
    /// `null` because it's still a block short.
    ///
    /// A rewritten request that gets no answer is retried as the client sent
    /// it: the rewrite relied on what was known at the moment (a cached
    /// height→hash that a reorg may have replaced), while the client's request
    /// is the source of truth.
    async fn execute_preferring(
        &self,
        request: &JsonRpcRequest,
        labels: &LabelSelector,
        prefer_height: Option<u64>,
    ) -> Result<Routed, UpstreamError> {
        // The quorum is always the client's method's, whatever the rewrite: it
        // carries the lag tolerance the client's call needs, and candidates
        // are those able to answer the client's call. One that can't take the
        // rewritten method refuses it, which leads to the retry below.
        let hint = self.quorum_for(&request.method).selector();
        let (plan, candidates) = {
            // Scanning every upstream for the head is only worth it for a
            // call that names a block; computed once when it does.
            let head = std::cell::OnceCell::new();
            let current_head = || *head.get_or_init(|| ChainAccess::current_height(self));
            let plan = self.planner.plan(request, &current_head);
            let candidates = self.candidates(
                hint,
                &request.method,
                plan.block,
                &current_head,
                labels,
                prefer_height,
            );
            (plan, candidates)
        };
        if plan.is_rewrite() {
            tracing::trace!(
                original = %request.method,
                rewritten = %plan.request.method,
                "request rewritten for routing"
            );
        }
        let routed = router::route(
            &self.chain,
            candidates.clone(),
            self.quorum_for(&request.method),
            &plan.request,
        )
        .await;
        if plan.is_rewrite() && rewrite_unanswered(&routed) {
            return router::route(
                &self.chain,
                candidates,
                self.quorum_for(&request.method),
                request,
            )
            .await;
        }
        routed
    }

    /// The upstreams that can take a call reading `block`.
    ///
    /// The quorum's lag tolerance is counted from the block the call reads,
    /// not from the chain head: a node a few blocks behind the head has every
    /// older block. Only a call at the head (or one that names no block)
    /// needs an upstream that's caught up with it.
    fn candidates(
        &self,
        hint: SelectorHint,
        method: &RpcMethod,
        block: Option<BlockRead>,
        head: &dyn Fn() -> Option<u64>,
        labels: &LabelSelector,
        prefer_height: Option<u64>,
    ) -> Vec<Arc<dyn RpcUpstream>> {
        let mut candidates = match (block, hint) {
            // Having the block is all that matters, checked below.
            (Some(BlockRead::State(_)), _) => self.select_available(method),
            (Some(BlockRead::Block(height)), SelectorHint::NotLagging { max_lag }) => {
                match head() {
                    // Capped at the head, so a block that doesn't exist yet is
                    // routed like one at the head, and gets its `null`.
                    Some(head) => {
                        self.select_reached(method, height.min(head).saturating_sub(max_lag))
                    }
                    None => self.select_for(hint, method),
                }
            }
            _ => self.select_for(hint, method),
        };
        if *labels != LabelSelector::Any {
            candidates.retain(|u| labels.matches_any_set(u.label_sets()));
        }
        // A hard filter: a node without the block would answer a state read
        // with wrong data, not an error, so no candidate must fail the call.
        if let Some(BlockRead::State(height)) = block {
            candidates = at_height(candidates, height);
        }
        if let Some(height) = prefer_height {
            let reached = at_height(candidates.clone(), height);
            // Best effort: with none there, the block may just not be
            // available anywhere yet.
            if !reached.is_empty() {
                candidates = reached;
            }
        }
        candidates
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

    /// Whether any upstream of this chain can answer the method — callable or
    /// hardcoded. The gate the legacy `VerifyingReader` applied before
    /// routing, distinguishing "unsupported method" from "no upstream
    /// available right now".
    pub fn method_available(&self, method: &RpcMethod) -> bool {
        self.quorum_factory.is_callable(method) || self.quorum_factory.is_hardcoded(method)
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

    /// Returns available upstreams that accept `method` and whose head has
    /// reached `height`. Reads the heads directly, so it's as current as they
    /// are. Upstreams that haven't reported a head yet are included, as in
    /// [`select_not_lagging`](Self::select_not_lagging).
    pub fn select_reached(&self, method: &RpcMethod, height: u64) -> Vec<Arc<dyn RpcUpstream>> {
        self.select_where(|u| {
            serves_rpc(u)
                && u.availability() <= UpstreamAvailability::Immature
                && u.allows_method(method)
                && u.head().current_height().is_none_or(|h| h >= height)
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

    fn serves_method(&self, method: &RpcMethod) -> bool {
        // Structural, not availability-based: the question is whether the
        // method can ever be routed here, the same gate `select_available`
        // applies per call.
        self.upstreams.iter().any(|u| u.allows_method(method))
    }

    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        self.execute(request, &LabelSelector::Any)
            .await
            .map(|routed| routed.response)
    }

    async fn call_at_height(
        &self,
        request: &JsonRpcRequest,
        min_height: u64,
    ) -> Result<JsonRpcResponse, UpstreamError> {
        self.execute_preferring(request, &LabelSelector::Any, Some(min_height))
            .await
            .map(|routed| routed.response)
    }
}

/// Whether a rewritten request's outcome may be due to the rewrite itself,
/// so the client's own request deserves a try: no data for the rewritten
/// form (a hash a reorg replaced answers `null`), or the method it was
/// rewritten to refused. A connection failure isn't one of those, and
/// retrying it would only double the wait.
fn rewrite_unanswered(routed: &Result<Routed, UpstreamError>) -> bool {
    match routed {
        Ok(routed) => !routed.response.is_non_empty_result(),
        Err(err) => matches!(err, UpstreamError::MethodNotAllowed(_)),
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
    fn select_reached_ignores_lag_and_checks_the_head() {
        let ms = ms_of(vec![
            // A stale lag must not matter: the head decides.
            MockUpstream::serving("behind-head", 95, serde_json::json!(null)),
            MockUpstream::serving("at-head", 100, serde_json::json!(null)),
            MockUpstream::serving("too-low", 50, serde_json::json!(null)),
        ]);

        assert_eq!(
            ids(&ms.select_reached(&"any".into(), 90)),
            vec!["behind-head", "at-head"]
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

    // ── Executing a planned call ──────────────────────────────────────

    use crate::cache::{CacheTag, Caches};
    use crate::data::{BlockContainer, BlockId};
    use crate::upstream::ethereum::call_plan::EthereumCallPlanner;
    use std::sync::Mutex;

    /// How the upstream answers `eth_getBlockByHash`; any other call gets a
    /// block.
    enum ByHash {
        Block,
        Null,
        Refused,
        Down,
    }

    /// Records the methods it's called with.
    struct RecordingUpstream {
        head: CurrentHead,
        state: Arc<UpstreamState>,
        by_hash: ByHash,
        calls: Mutex<Vec<String>>,
    }

    impl RecordingUpstream {
        fn new(by_hash: ByHash) -> Arc<Self> {
            let head = CurrentHead::new();
            head.update(100);
            Arc::new(Self {
                head,
                state: Arc::new(UpstreamState::new()),
                by_hash,
                calls: Mutex::new(Vec::new()),
            })
        }

        fn methods_called(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for RecordingUpstream {
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let method = request.method.as_str();
            self.calls.lock().unwrap().push(method.to_string());
            let body = match (method, &self.by_hash) {
                ("eth_getBlockByHash", ByHash::Null) => r#"{"jsonrpc":"2.0","id":1,"result":null}"#,
                ("eth_getBlockByHash", ByHash::Refused) => {
                    return Err(UpstreamError::MethodNotAllowed(method.to_string()));
                }
                ("eth_getBlockByHash", ByHash::Down) => {
                    return Err(UpstreamError::Transport("connection refused".into()));
                }
                _ => r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x64"}}"#,
            };
            Ok(serde_json::from_str(body).unwrap())
        }
        fn id(&self) -> &UpstreamId {
            crate::upstream::id::stub_id()
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::Ok
        }
        fn head(&self) -> &dyn Head {
            &self.head
        }
        fn lag(&self) -> Option<u64> {
            Some(0)
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
    }

    /// A chain whose cache knows the hash of the block at 100, the head.
    fn chain_with_cached_head(upstream: Arc<RecordingUpstream>) -> Multistream {
        let caches = Caches::new();
        caches.cache(
            CacheTag::Latest,
            BlockContainer {
                hash: BlockId::from_bytes([7u8; 32]),
                height: 100,
                parent_hash: None,
                total_difficulty: alloy::primitives::U256::ZERO,
                timestamp: jiff::Timestamp::UNIX_EPOCH,
                transaction_hashes: vec![],
                json: None,
                header_json: None,
            },
        );
        Multistream::new(test_chain(), vec![upstream], Arc::new(DefaultMethods))
            .with_planner(Arc::new(EthereumCallPlanner::new(Arc::new(caches))))
    }

    fn latest_block() -> JsonRpcRequest {
        JsonRpcRequest::new(
            1,
            "eth_getBlockByNumber".into(),
            serde_json::json!(["latest", false]),
        )
    }

    #[tokio::test]
    async fn sends_the_planned_request() {
        let upstream = RecordingUpstream::new(ByHash::Block);
        let ms = chain_with_cached_head(Arc::clone(&upstream));

        let routed = ms
            .execute(&latest_block(), &LabelSelector::Any)
            .await
            .unwrap();

        assert!(routed.response.is_non_empty_result());
        assert_eq!(upstream.methods_called(), vec!["eth_getBlockByHash"]);
    }

    #[tokio::test]
    async fn rewrite_answered_with_null_falls_back_to_the_clients_request() {
        // The cached hash may have been replaced by a reorg.
        let upstream = RecordingUpstream::new(ByHash::Null);
        let ms = chain_with_cached_head(Arc::clone(&upstream));

        let routed = ms
            .execute(&latest_block(), &LabelSelector::Any)
            .await
            .unwrap();

        assert!(routed.response.is_non_empty_result());
        assert_eq!(
            upstream.methods_called(),
            vec!["eth_getBlockByHash", "eth_getBlockByNumber"]
        );
    }

    #[tokio::test]
    async fn rewrite_refused_falls_back_to_the_clients_request() {
        let upstream = RecordingUpstream::new(ByHash::Refused);
        let ms = chain_with_cached_head(Arc::clone(&upstream));

        let routed = ms
            .execute(&latest_block(), &LabelSelector::Any)
            .await
            .unwrap();

        assert!(routed.response.is_non_empty_result());
        assert_eq!(
            upstream.methods_called(),
            vec!["eth_getBlockByHash", "eth_getBlockByNumber"]
        );
    }

    #[tokio::test]
    async fn rewrite_failing_on_connection_is_not_retried() {
        // Nothing the client's own request would fix; a retry would only
        // double the wait.
        let upstream = RecordingUpstream::new(ByHash::Down);
        let ms = chain_with_cached_head(Arc::clone(&upstream));

        let result = ms.execute(&latest_block(), &LabelSelector::Any).await;

        assert!(matches!(result, Err(UpstreamError::Transport(_))));
        assert_eq!(upstream.methods_called(), vec!["eth_getBlockByHash"]);
    }

    #[tokio::test]
    async fn call_at_height_is_planned_too() {
        // Internal block reads (fee estimation) get the same rewrite to a
        // cacheable by-hash read as client calls.
        let upstream = RecordingUpstream::new(ByHash::Block);
        let ms = chain_with_cached_head(Arc::clone(&upstream));
        let request = JsonRpcRequest::new(
            1,
            "eth_getBlockByNumber".into(),
            serde_json::json!(["0x64", true]),
        );

        ms.call_at_height(&request, 100).await.unwrap();

        assert_eq!(upstream.methods_called(), vec!["eth_getBlockByHash"]);
    }

    #[tokio::test]
    async fn chain_without_a_planner_sends_the_request_as_is() {
        let upstream = RecordingUpstream::new(ByHash::Block);
        let ms = Multistream::new(
            test_chain(),
            vec![Arc::clone(&upstream) as Arc<dyn RpcUpstream>],
            Arc::new(DefaultMethods),
        );

        ms.execute(&latest_block(), &LabelSelector::Any)
            .await
            .unwrap();

        assert_eq!(upstream.methods_called(), vec!["eth_getBlockByNumber"]);
    }
}
