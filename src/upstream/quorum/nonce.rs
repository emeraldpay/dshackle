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

//! `NonceQuorum` — query several upstreams for a transaction count and
//! return the largest value seen.
//!
//! Used for `eth_getTransactionCount`. Different upstreams may have seen
//! different mempool states, and reporting a stale (lower) nonce would cause
//! the next transaction to be rejected. Picking the maximum minimizes that
//! risk at the cost of an extra round-trip or two.

use crate::jsonrpc::JsonRpcResponse;
use crate::upstream::ethereum::parse_hex_quantity;
use crate::upstream::quorum::{CallQuorum, QuorumOutcome};
use crate::upstream::traits::{RpcUpstream, UpstreamError};

const DEFAULT_TRIES: usize = 3;

/// Polls `tries` upstreams; returns the response carrying the highest nonce.
pub struct NonceQuorum {
    tries: usize,
    received: usize,
    errors: usize,
    /// Best response seen so far, kept verbatim to forward to the client.
    best_response: Option<(JsonRpcResponse, String)>,
    resolved_source: Option<String>,
    /// Numeric value of `best_response` for comparisons.
    best_value: u64,
    last_error: Option<UpstreamError>,
}

impl NonceQuorum {
    pub fn new() -> Self {
        Self::with_tries(DEFAULT_TRIES)
    }

    pub fn with_tries(tries: usize) -> Self {
        Self {
            tries: tries.max(1),
            received: 0,
            errors: 0,
            best_response: None,
            resolved_source: None,
            best_value: 0,
            last_error: None,
        }
    }
}

impl Default for NonceQuorum {
    fn default() -> Self {
        Self::new()
    }
}

impl CallQuorum for NonceQuorum {
    fn set_total_upstreams(&mut self, total: usize) {
        self.tries = self.tries.min(total).max(1);
    }

    fn record_response(&mut self, response: JsonRpcResponse, upstream: &dyn RpcUpstream) {
        self.received += 1;
        let value = response
            .result_as_string()
            .and_then(|s| parse_hex_quantity(&s));
        match value {
            Some(v) if v >= self.best_value || self.best_response.is_none() => {
                self.best_value = v;
                self.best_response = Some((response, upstream.id().to_string()));
            }
            _ => {
                // Keep whatever we have; if nothing yet, store as a fallback.
                if self.best_response.is_none() {
                    self.best_response = Some((response, upstream.id().to_string()));
                }
            }
        }
    }

    fn record_error(&mut self, error: &UpstreamError, _upstream: &dyn RpcUpstream) {
        self.errors += 1;
        self.last_error = Some(error.clone());
    }

    fn is_resolved(&self) -> bool {
        self.received >= self.tries && !self.is_failed()
    }

    fn is_failed(&self) -> bool {
        self.errors >= self.tries
    }

    fn take_outcome(&mut self) -> QuorumOutcome {
        if let Some((r, source)) = self.best_response.take() {
            self.resolved_source = Some(source);
            return QuorumOutcome::Resolved(r);
        }
        if let Some(e) = self.last_error.take() {
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

    fn parse(body: &str) -> JsonRpcResponse {
        serde_json::from_str(body).unwrap()
    }

    #[test]
    fn picks_highest_nonce_across_responses() {
        let mut q = NonceQuorum::with_tries(3);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0x5"}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xa"}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0x7"}"#),
            &StubUpstream,
        );

        assert!(q.is_resolved());
        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""0xa""#);
            }
            _ => panic!("expected Resolved"),
        }
    }

    #[test]
    fn waits_for_all_tries_before_resolving() {
        let mut q = NonceQuorum::with_tries(3);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0x5"}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xa"}"#),
            &StubUpstream,
        );
        assert!(!q.is_resolved(), "should wait for the third response");
    }

    #[test]
    fn fails_when_all_tries_error() {
        let mut q = NonceQuorum::with_tries(2);
        q.record_error(&UpstreamError::HttpStatus(503), &StubUpstream);
        q.record_error(&UpstreamError::HttpStatus(503), &StubUpstream);

        assert!(q.is_failed());
        match q.take_outcome() {
            QuorumOutcome::Failed(UpstreamError::HttpStatus(503)) => {}
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn tries_clamped_to_total_upstreams() {
        let mut q = NonceQuorum::with_tries(10);
        q.set_total_upstreams(2);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0x2"}"#),
            &StubUpstream,
        );
        assert!(q.is_resolved());
    }

    #[test]
    fn errors_do_not_consume_try_budget() {
        // Legacy "Ignore error": a transport error shouldn't count as a
        // received response — the quorum still needs `tries` valid answers.
        let mut q = NonceQuorum::with_tries(3);
        q.record_error(&UpstreamError::HttpStatus(503), &StubUpstream);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0x10"}"#),
            &StubUpstream,
        );
        assert!(!q.is_resolved());
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0x11"}"#),
            &StubUpstream,
        );
        assert!(
            !q.is_resolved(),
            "two successes are not enough after an error"
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0x10"}"#),
            &StubUpstream,
        );
        assert!(q.is_resolved());

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""0x11""#);
            }
            _ => panic!("expected Resolved with max nonce"),
        }
    }

    #[test]
    fn malformed_response_does_not_overwrite_best() {
        let mut q = NonceQuorum::with_tries(2);
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"0xa"}"#),
            &StubUpstream,
        );
        q.record_response(
            parse(r#"{"jsonrpc":"2.0","id":1,"result":"garbage"}"#),
            &StubUpstream,
        );

        match q.take_outcome() {
            QuorumOutcome::Resolved(r) => {
                assert_eq!(r.result.unwrap().get(), r#""0xa""#);
            }
            _ => panic!("expected Resolved with the parseable nonce"),
        }
    }
}
