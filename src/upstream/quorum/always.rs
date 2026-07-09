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

//! `AlwaysQuorum` — accept the first response from any upstream.
//!
//! This is the default strategy for read-only calls where any single upstream
//! answer is acceptable. Connection-style errors (HTTP 401/429/5xx, transport
//! failures) are held back as a tentative error: the router will move on to
//! another upstream, and the held-back error is only surfaced if no other
//! upstream produces a real answer.

use crate::jsonrpc::JsonRpcResponse;
use crate::upstream::quorum::{CallQuorum, QuorumOutcome, is_connection_unavailable};
use crate::upstream::traits::{RpcUpstream, UpstreamError};

/// Resolves on the first response received. Definitive errors fail immediately;
/// connection errors are deferred until no upstream succeeds.
#[derive(Default)]
pub struct AlwaysQuorum {
    result: Option<(JsonRpcResponse, String)>,
    resolved_source: Option<String>,
    error: Option<UpstreamError>,
    /// Connection errors are only surfaced after all upstreams have been
    /// exhausted, so a single flaky node doesn't fail an otherwise routable
    /// request.
    deferred_error: Option<UpstreamError>,
}

impl AlwaysQuorum {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CallQuorum for AlwaysQuorum {
    fn set_total_upstreams(&mut self, _total: usize) {}

    fn record_response(&mut self, response: JsonRpcResponse, upstream: &dyn RpcUpstream) {
        if self.result.is_none() {
            self.result = Some((response, upstream.id().to_string()));
        }
    }

    fn record_error(&mut self, error: &UpstreamError, _upstream: &dyn RpcUpstream) {
        if is_connection_unavailable(error) {
            // Hold back; another upstream may still succeed.
            if self.deferred_error.is_none() {
                self.deferred_error = Some(error.clone());
            }
        } else {
            self.error = Some(error.clone());
        }
    }

    fn is_resolved(&self) -> bool {
        self.result.is_some()
    }

    fn is_failed(&self) -> bool {
        self.error.is_some()
    }

    fn close(&mut self) {
        if self.result.is_none() && self.error.is_none() {
            self.error = self.deferred_error.take();
        }
    }

    fn take_outcome(&mut self) -> QuorumOutcome {
        if let Some((r, source)) = self.result.take() {
            self.resolved_source = Some(source);
            return QuorumOutcome::Resolved(r);
        }
        if let Some(e) = self.error.take() {
            return QuorumOutcome::Failed(e);
        }
        QuorumOutcome::Empty
    }

    fn resolved_by(&self) -> Option<&str> {
        self.resolved_source.as_deref()
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
        let mut q = AlwaysQuorum::new();
        assert!(!q.is_resolved());

        q.record_response(
            make_response(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#),
            &StubUpstream,
        );

        assert!(q.is_resolved());
        assert!(!q.is_failed());

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""0x1""#);
            }
            _ => panic!("expected Resolved"),
        }
    }

    #[test]
    fn second_response_is_ignored() {
        let mut q = AlwaysQuorum::new();
        q.record_response(
            make_response(r#"{"jsonrpc":"2.0","id":1,"result":"first"}"#),
            &StubUpstream,
        );
        q.record_response(
            make_response(r#"{"jsonrpc":"2.0","id":1,"result":"second"}"#),
            &StubUpstream,
        );

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""first""#);
            }
            _ => panic!("expected Resolved"),
        }
    }

    #[test]
    fn definitive_error_fails_quorum() {
        let mut q = AlwaysQuorum::new();
        q.record_error(
            &UpstreamError::InvalidResponse("garbage".into()),
            &StubUpstream,
        );

        assert!(q.is_failed());
        match q.take_outcome() {
            QuorumOutcome::Failed(UpstreamError::InvalidResponse(_)) => {}
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn connection_error_is_deferred() {
        let mut q = AlwaysQuorum::new();
        q.record_error(&UpstreamError::HttpStatus(429), &StubUpstream);

        assert!(!q.is_failed(), "should not fail on connection error alone");
        assert!(!q.is_resolved());
    }

    #[test]
    fn deferred_error_surfaces_on_close() {
        let mut q = AlwaysQuorum::new();
        q.record_error(&UpstreamError::HttpStatus(429), &StubUpstream);
        q.close();

        assert!(q.is_failed());
        match q.take_outcome() {
            QuorumOutcome::Failed(UpstreamError::HttpStatus(429)) => {}
            _ => panic!("expected Failed(429)"),
        }
    }

    #[test]
    fn deferred_error_does_not_override_response() {
        let mut q = AlwaysQuorum::new();
        q.record_error(&UpstreamError::HttpStatus(503), &StubUpstream);
        q.record_response(
            make_response(r#"{"jsonrpc":"2.0","id":1,"result":"ok"}"#),
            &StubUpstream,
        );
        q.close();

        match q.take_outcome() {
            QuorumOutcome::Resolved(_) => {}
            _ => panic!("expected Resolved"),
        }
    }

    #[test]
    fn empty_quorum_returns_empty_outcome() {
        let mut q = AlwaysQuorum::new();
        match q.take_outcome() {
            QuorumOutcome::Empty => {}
            _ => panic!("expected Empty"),
        }
    }

    #[test]
    fn jsonrpc_error_response_is_treated_as_resolved() {
        // The upstream gave a definitive answer (a JSON-RPC error). The quorum
        // accepts it; the caller is responsible for forwarding the error to
        // the client.
        let mut q = AlwaysQuorum::new();
        q.record_response(
            make_response(
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"unknown method"}}"#,
            ),
            &StubUpstream,
        );
        assert!(q.is_resolved());
    }
}
