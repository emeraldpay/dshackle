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

//! `BroadcastQuorum` — submit a transaction to multiple upstreams to maximize
//! propagation, then return the first txid we got back (or, if no upstream
//! accepted it, the most informative error response).
//!
//! Used for `eth_sendRawTransaction` and `sendrawtransaction`. After
//! `target_calls` upstreams have been attempted, the quorum resolves with
//! whichever response is most useful to the client.

use crate::jsonrpc::JsonRpcResponse;
use crate::upstream::quorum::{CallQuorum, QuorumOutcome};
use crate::upstream::traits::{RpcUpstream, UpstreamError};

const DEFAULT_TARGET: usize = 3;

/// Broadcasts to up to `target_calls` upstreams; resolves once that many
/// have answered. Mirrors the legacy `BroadcastQuorum`.
pub struct BroadcastQuorum {
    target_calls: usize,
    calls: usize,
    /// First successful txid response, kept verbatim to forward to the client.
    txid_response: Option<JsonRpcResponse>,
    /// First non-success response (e.g. "transaction already known"). Used as
    /// a fallback when no upstream accepted the broadcast — the client is
    /// better served by the upstream's error message than by a generic one.
    fallback_response: Option<JsonRpcResponse>,
    /// Last transport-level error, used only when nothing else is available.
    last_transport_error: Option<UpstreamError>,
}

impl BroadcastQuorum {
    /// Construct a broadcast quorum with the default target of 3 upstreams.
    pub fn new() -> Self {
        Self::with_target(DEFAULT_TARGET)
    }

    /// Construct a broadcast quorum with a custom target. The actual number
    /// of attempts is clamped to the available upstream count by
    /// `set_total_upstreams`.
    pub fn with_target(target_calls: usize) -> Self {
        Self {
            target_calls: target_calls.max(1),
            calls: 0,
            txid_response: None,
            fallback_response: None,
            last_transport_error: None,
        }
    }
}

impl Default for BroadcastQuorum {
    fn default() -> Self {
        Self::new()
    }
}

impl CallQuorum for BroadcastQuorum {
    fn set_total_upstreams(&mut self, total: usize) {
        self.target_calls = self.target_calls.min(total).max(1);
    }

    fn record_response(&mut self, response: JsonRpcResponse, _upstream: &dyn RpcUpstream) {
        self.calls += 1;
        // A successful broadcast returns the txid as a JSON string; anything
        // else (error response, non-string result) is a fallback.
        if self.txid_response.is_none() && response.result_as_string().is_some() {
            self.txid_response = Some(response);
        } else if self.fallback_response.is_none() {
            self.fallback_response = Some(response);
        }
    }

    fn record_error(&mut self, error: &UpstreamError, _upstream: &dyn RpcUpstream) {
        self.calls += 1;
        self.last_transport_error = Some(error.clone());
    }

    fn is_resolved(&self) -> bool {
        self.calls >= self.target_calls && self.txid_response.is_some()
    }

    fn is_failed(&self) -> bool {
        // Only "failed" once we've made all the attempts and none gave a txid.
        self.calls >= self.target_calls && self.txid_response.is_none()
    }

    fn take_outcome(&mut self) -> QuorumOutcome {
        if let Some(r) = self.txid_response.take() {
            return QuorumOutcome::Resolved(r);
        }
        // No upstream returned a txid. Prefer to forward the upstream's own
        // error response (it carries useful detail like "nonce too low") over
        // a synthetic transport error.
        if let Some(r) = self.fallback_response.take() {
            return QuorumOutcome::Resolved(r);
        }
        if let Some(e) = self.last_transport_error.take() {
            return QuorumOutcome::Failed(e);
        }
        QuorumOutcome::Empty
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
        fn id(&self) -> &str { "stub" }
        fn availability(&self) -> UpstreamAvailability { UpstreamAvailability::Ok }
        fn head(&self) -> &dyn Head { &NoHead }
        fn lag(&self) -> Option<u64> { None }
        fn state(&self) -> &Arc<UpstreamState> { &MOCK_STATE }
    }

    fn parse(body: &str) -> JsonRpcResponse {
        serde_json::from_str(body).unwrap()
    }

    #[test]
    fn resolves_after_target_calls_with_txid() {
        let mut q = BroadcastQuorum::with_target(2);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xdeadbeef"}"#),
            &StubUpstream,
        );
        assert!(!q.is_resolved(), "needs all calls before resolving");
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xdeadbeef"}"#),
            &StubUpstream,
        );
        assert!(q.is_resolved());

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""0xdeadbeef""#);
            }
            _ => panic!("expected Resolved"),
        }
    }

    #[test]
    fn first_txid_wins_even_if_others_error() {
        let mut q = BroadcastQuorum::with_target(2);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xtxid"}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"already known"}}"#),
            &StubUpstream,
        );

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""0xtxid""#);
            }
            _ => panic!("expected Resolved with txid"),
        }
    }

    #[test]
    fn forwards_error_response_when_no_upstream_accepted() {
        let mut q = BroadcastQuorum::with_target(2);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low"}}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low"}}"#),
            &StubUpstream,
        );
        assert!(q.is_failed());

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                // Forwarded so the client sees the upstream's actual error message.
                assert_eq!(r.error.unwrap().message, "nonce too low");
            }
            _ => panic!("expected Resolved with the error response forwarded"),
        }
    }

    #[test]
    fn falls_back_to_transport_error_when_all_calls_fail() {
        let mut q = BroadcastQuorum::with_target(2);
        q.record_error(&UpstreamError::HttpStatus(503), &StubUpstream);
        q.record_error(&UpstreamError::HttpStatus(503), &StubUpstream);
        assert!(q.is_failed());

        match q.take_outcome() {
            QuorumOutcome::Failed(UpstreamError::HttpStatus(503)) => {}
            _ => panic!("expected transport failure"),
        }
    }

    #[test]
    fn target_clamped_to_total_upstreams() {
        let mut q = BroadcastQuorum::with_target(10);
        q.set_total_upstreams(2);

        // Only two responses needed
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xtxid"}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xtxid"}"#),
            &StubUpstream,
        );
        assert!(q.is_resolved());
    }

    #[test]
    fn captures_success_after_prior_error_response() {
        // Legacy "Use first valid": the first upstream returned an error,
        // the second returned a txid, the third errored. The txid still wins.
        let mut q = BroadcastQuorum::with_target(3);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"Internal"}}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xtxid"}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"Nonce too low"}}"#),
            &StubUpstream,
        );
        assert!(q.is_resolved());

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""0xtxid""#);
            }
            _ => panic!("expected Resolved with txid"),
        }
    }

    #[test]
    fn target_clamped_to_at_least_one() {
        let mut q = BroadcastQuorum::with_target(3);
        q.set_total_upstreams(0);

        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xtxid"}"#),
            &StubUpstream,
        );
        assert!(q.is_resolved());
    }
}
