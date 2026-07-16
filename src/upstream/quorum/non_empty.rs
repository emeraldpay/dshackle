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

//! `NonEmptyQuorum` — retry until an upstream returns a non-null result.
//!
//! Used for methods like `eth_getTransactionByHash` where a fresh transaction
//! may not yet be visible on every node. The quorum stops on the first
//! non-null result, on the first definitive error, or after `max_tries`
//! upstreams have been polled.

use crate::jsonrpc::JsonRpcResponse;
use crate::upstream::id::UpstreamId;
use crate::upstream::quorum::{CallQuorum, QuorumOutcome};
use crate::upstream::traits::{RpcUpstream, UpstreamError};

const DEFAULT_MAX_TRIES: usize = 3;

/// Resolves on the first non-null result, fails after `max_tries` empties.
pub struct NonEmptyQuorum {
    max_tries: usize,
    tries: usize,
    /// First non-null response seen.
    result: Option<(JsonRpcResponse, UpstreamId)>,
    /// Fallback for when every upstream returned null — better to forward a
    /// real `null` than a synthetic transport error.
    last_empty_response: Option<(JsonRpcResponse, UpstreamId)>,
    resolved_source: Option<UpstreamId>,
    last_error: Option<UpstreamError>,
}

impl NonEmptyQuorum {
    pub fn new() -> Self {
        Self::with_max_tries(DEFAULT_MAX_TRIES)
    }

    pub fn with_max_tries(max_tries: usize) -> Self {
        Self {
            max_tries: max_tries.max(1),
            tries: 0,
            result: None,
            last_empty_response: None,
            resolved_source: None,
            last_error: None,
        }
    }
}

impl Default for NonEmptyQuorum {
    fn default() -> Self {
        Self::new()
    }
}

impl CallQuorum for NonEmptyQuorum {
    fn set_total_upstreams(&mut self, _total: usize) {
        // Unlike Broadcast, NonEmpty does not clamp to upstream count: it
        // intentionally keeps retrying because the data may arrive shortly.
    }

    fn record_response(&mut self, response: JsonRpcResponse, upstream: &dyn RpcUpstream) {
        self.tries += 1;
        if response.is_non_empty_result() {
            if self.result.is_none() {
                self.result = Some((response, upstream.id().clone()));
            }
        } else if self.last_empty_response.is_none() {
            self.last_empty_response = Some((response, upstream.id().clone()));
        }
    }

    fn record_error(&mut self, error: &UpstreamError, _upstream: &dyn RpcUpstream) {
        self.tries += 1;
        self.last_error = Some(error.clone());
    }

    fn is_resolved(&self) -> bool {
        self.result.is_some()
    }

    fn is_failed(&self) -> bool {
        self.tries >= self.max_tries && self.result.is_none()
    }

    fn take_outcome(&mut self) -> QuorumOutcome {
        if let Some((r, source)) = self.result.take() {
            self.resolved_source = Some(source);
            return QuorumOutcome::Resolved(r);
        }
        // Forward a real null result to the client rather than fabricating one.
        if let Some((r, source)) = self.last_empty_response.take() {
            self.resolved_source = Some(source);
            return QuorumOutcome::Resolved(r);
        }
        if let Some(e) = self.last_error.take() {
            return QuorumOutcome::Failed(e);
        }
        QuorumOutcome::Empty
    }

    fn resolved_by(&self) -> Option<&UpstreamId> {
        self.resolved_source.as_ref()
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
        fn id(&self) -> &UpstreamId {
            crate::upstream::id::stub_id()
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

    fn parse(body: &str) -> JsonRpcResponse {
        serde_json::from_str(body).unwrap()
    }

    #[test]
    fn first_non_null_response_resolves() {
        let mut q = NonEmptyQuorum::new();
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":null}"#),
            &StubUpstream,
        );
        assert!(!q.is_resolved());

        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":{"hash":"0xabc"}}"#),
            &StubUpstream,
        );
        assert!(q.is_resolved());
    }

    #[test]
    fn fails_after_max_tries_of_null() {
        let mut q = NonEmptyQuorum::with_max_tries(2);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":null}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":null}"#),
            &StubUpstream,
        );
        assert!(q.is_failed());
        // Forward the real null to the client.
        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => assert!(r.result.is_none()),
            _ => panic!("expected Resolved (forwarded null)"),
        }
    }

    #[test]
    fn returns_response_when_one_upstream_has_it() {
        let mut q = NonEmptyQuorum::with_max_tries(3);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":null}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xfound"}"#),
            &StubUpstream,
        );

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""0xfound""#);
            }
            _ => panic!("expected Resolved"),
        }
    }

    #[test]
    fn resolves_on_success_after_error() {
        // Legacy "Result with the first after error": a transport error on
        // the first upstream shouldn't prevent a later non-null response
        // from resolving the quorum.
        let mut q = NonEmptyQuorum::with_max_tries(3);
        q.record_error(&UpstreamError::HttpStatus(503), &StubUpstream);
        assert!(!q.is_resolved());
        assert!(!q.is_failed());

        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0x11"}"#),
            &StubUpstream,
        );
        assert!(q.is_resolved());

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""0x11""#);
            }
            _ => panic!("expected Resolved"),
        }
    }

    #[test]
    fn transport_errors_count_against_max_tries() {
        let mut q = NonEmptyQuorum::with_max_tries(2);
        q.record_error(&UpstreamError::HttpStatus(503), &StubUpstream);
        q.record_error(&UpstreamError::HttpStatus(503), &StubUpstream);
        assert!(q.is_failed());

        match q.take_outcome() {
            QuorumOutcome::Failed(UpstreamError::HttpStatus(503)) => {}
            _ => panic!("expected Failed transport error"),
        }
    }
}
