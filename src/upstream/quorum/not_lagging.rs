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

//! `NotLaggingQuorum` — accept a response only from an upstream that's
//! sufficiently caught up to the chain head.
//!
//! Used for queries whose correctness depends on having current state
//! (`eth_blockNumber`, `eth_getBalance`, head-block reads). Behaviorally
//! identical to [`AlwaysQuorum`]; the difference is the
//! [`SelectorHint::NotLagging`] hint, which makes the [`Multistream`]
//! pre-filter candidates by lag before the router starts calling them.
//!
//! [`Multistream`]: crate::upstream::Multistream

use crate::jsonrpc::JsonRpcResponse;
use crate::upstream::quorum::{AlwaysQuorum, CallQuorum, QuorumOutcome, SelectorHint};
use crate::upstream::traits::{RpcUpstream, UpstreamError};

/// Resolves on the first response from a non-lagging upstream. The lag check
/// itself happens at selection time via the [`SelectorHint::NotLagging`]
/// returned by [`selector`](Self::selector); the call-handling logic
/// delegates to [`AlwaysQuorum`].
pub struct NotLaggingQuorum {
    max_lag: u64,
    inner: AlwaysQuorum,
}

impl NotLaggingQuorum {
    pub fn new(max_lag: u64) -> Self {
        Self {
            max_lag,
            inner: AlwaysQuorum::new(),
        }
    }
}

impl CallQuorum for NotLaggingQuorum {
    fn set_total_upstreams(&mut self, total: usize) {
        self.inner.set_total_upstreams(total);
    }

    fn record_response(&mut self, response: JsonRpcResponse, upstream: &dyn RpcUpstream) {
        self.inner.record_response(response, upstream);
    }

    fn record_error(&mut self, error: &UpstreamError, upstream: &dyn RpcUpstream) {
        self.inner.record_error(error, upstream);
    }

    fn is_resolved(&self) -> bool {
        self.inner.is_resolved()
    }

    fn is_failed(&self) -> bool {
        self.inner.is_failed()
    }

    fn close(&mut self) {
        self.inner.close();
    }

    fn take_outcome(&mut self) -> QuorumOutcome {
        self.inner.take_outcome()
    }

    fn selector(&self) -> SelectorHint {
        SelectorHint::NotLagging {
            max_lag: self.max_lag,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::JsonRpcRequest;
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::state::UpstreamState;
    use std::sync::Arc;

    static MOCK_STATE: std::sync::LazyLock<Arc<UpstreamState>> =
        std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));

    struct StubUpstream;

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            unimplemented!()
        }
        fn id(&self) -> &str {
            "stub"
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::Ok
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            None
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &MOCK_STATE
        }
    }

    fn make_response(body: &str) -> JsonRpcResponse {
        serde_json::from_str(body).unwrap()
    }

    #[test]
    fn first_response_resolves_quorum() {
        let mut q = NotLaggingQuorum::new(0);
        q.record_response(
            make_response(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#),
            &StubUpstream,
        );
        assert!(q.is_resolved());
    }

    #[test]
    fn definitive_error_fails_quorum() {
        let mut q = NotLaggingQuorum::new(0);
        q.record_error(
            &UpstreamError::InvalidResponse("garbage".into()),
            &StubUpstream,
        );
        assert!(q.is_failed());
    }

    #[test]
    fn deferred_connection_error_surfaces_on_close() {
        let mut q = NotLaggingQuorum::new(1);
        q.record_error(&UpstreamError::HttpStatus(429), &StubUpstream);
        assert!(!q.is_failed());
        q.close();
        assert!(q.is_failed());
    }

    #[test]
    fn selector_reports_max_lag() {
        let q = NotLaggingQuorum::new(4);
        match q.selector() {
            SelectorHint::NotLagging { max_lag: 4 } => {}
            other => panic!("unexpected selector: {other:?}"),
        }
    }
}
