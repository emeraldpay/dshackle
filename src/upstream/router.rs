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

//! Routes a single JSON-RPC request through a list of candidate upstreams,
//! driven by a `CallQuorum` policy.
//!
//! Counterpart of the legacy `QuorumRpcReader`:
//!
//! 1. Caller picks the candidate list (using a `Multistream` selector matched
//!    to the quorum's [`SelectorHint`](super::quorum::SelectorHint)).
//! 2. Router calls each candidate in turn, feeding the outcome to the quorum.
//! 3. Loop stops as soon as `is_resolved` or `is_failed` returns true, or
//!    when the candidate list is exhausted.
//! 4. `close()` runs, then the quorum's accumulated outcome is returned.
//!
//! There is no separate retry cap — each quorum carries its own termination
//! logic (`AlwaysQuorum` stops on first success, `BroadcastQuorum` stops at
//! the configured count, etc.).

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::quorum::{CallQuorum, QuorumOutcome};
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// A response accepted by the quorum, together with the id of the upstream
/// that produced it (needed to attribute the response, e.g. for signing).
#[derive(Debug)]
pub struct Routed {
    pub response: JsonRpcResponse,
    pub source: Option<String>,
}

/// Execute `request` against the given `candidates` using the given quorum
/// strategy.
///
/// `candidates` should already be filtered to those the quorum is willing to
/// consider — typically obtained from `Multistream::select_for(quorum.selector())`.
pub async fn route(
    candidates: Vec<Arc<dyn RpcUpstream>>,
    mut quorum: Box<dyn CallQuorum>,
    request: &JsonRpcRequest,
) -> Result<Routed, UpstreamError> {
    quorum.set_total_upstreams(candidates.len());

    if candidates.is_empty() {
        return Err(UpstreamError::Transport(
            "no available upstream for request".into(),
        ));
    }

    for upstream in candidates {
        match upstream.call(request).await {
            Ok(response) => {
                quorum.record_response(response, upstream.as_ref());
            }
            Err(err) => {
                quorum.record_error(&err, upstream.as_ref());
            }
        }
        if quorum.is_resolved() || quorum.is_failed() {
            break;
        }
    }

    quorum.close();

    match quorum.take_outcome() {
        QuorumOutcome::Resolved(r) => Ok(Routed {
            response: r,
            source: quorum.resolved_by().map(str::to_string),
        }),
        QuorumOutcome::Failed(e) => Err(e),
        QuorumOutcome::Empty => Err(UpstreamError::Transport(
            "no upstream produced a response".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::quorum::AlwaysQuorum;
    use crate::upstream::state::UpstreamState;
    use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

    /// Mock upstream that returns a scripted sequence of outcomes.
    struct ScriptedUpstream {
        label: String,
        availability: AtomicU8,
        calls: AtomicU32,
        outcomes: std::sync::Mutex<Vec<Result<String, UpstreamError>>>,
        state: Arc<UpstreamState>,
    }

    impl ScriptedUpstream {
        fn new(label: &str, outcomes: Vec<Result<String, UpstreamError>>) -> Arc<Self> {
            Arc::new(Self {
                label: label.to_string(),
                availability: AtomicU8::new(UpstreamAvailability::Ok as u8),
                calls: AtomicU32::new(0),
                outcomes: std::sync::Mutex::new(outcomes),
                state: Arc::new(UpstreamState::new()),
            })
        }

        fn ok(label: &str, body: &str) -> Arc<Self> {
            Self::new(label, vec![Ok(body.to_string())])
        }

        fn err(label: &str, err: UpstreamError) -> Arc<Self> {
            Self::new(label, vec![Err(err)])
        }

        fn call_count(&self) -> u32 {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for ScriptedUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut outcomes = self.outcomes.lock().unwrap();
            let outcome = if outcomes.len() > 1 {
                outcomes.remove(0)
            } else {
                outcomes.last().cloned().unwrap()
            };
            match outcome {
                Ok(body) => Ok(serde_json::from_str(&body).unwrap()),
                Err(e) => Err(e),
            }
        }
        fn id(&self) -> &str {
            &self.label
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::from_u8(self.availability.load(Ordering::Relaxed))
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            None
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
    }

    fn dummy_request() -> JsonRpcRequest {
        JsonRpcRequest::new(1, "eth_blockNumber".into(), serde_json::json!([]))
    }

    fn vec_of(probes: &[Arc<ScriptedUpstream>]) -> Vec<Arc<dyn RpcUpstream>> {
        probes
            .iter()
            .map(|p| p.clone() as Arc<dyn RpcUpstream>)
            .collect()
    }

    #[tokio::test]
    async fn returns_first_successful_response() {
        let a = ScriptedUpstream::ok("a", r#"{"jsonrpc":"2.0","id":1,"result":"0xa"}"#);
        let b = ScriptedUpstream::ok("b", r#"{"jsonrpc":"2.0","id":1,"result":"0xb"}"#);

        let resp = route(
            vec_of(&[a.clone(), b.clone()]),
            Box::new(AlwaysQuorum::new()),
            &dummy_request(),
        )
        .await
        .unwrap();

        assert_eq!(resp.response.result.unwrap().get(), r#""0xa""#);
        assert_eq!(resp.source.as_deref(), Some("a"));
        assert_eq!(a.call_count(), 1);
        assert_eq!(b.call_count(), 0);
    }

    #[tokio::test]
    async fn falls_through_connection_error_to_next_upstream() {
        let a = ScriptedUpstream::err("a", UpstreamError::HttpStatus(429));
        let b = ScriptedUpstream::ok("b", r#"{"jsonrpc":"2.0","id":1,"result":"0xb"}"#);

        let resp = route(
            vec_of(&[a.clone(), b.clone()]),
            Box::new(AlwaysQuorum::new()),
            &dummy_request(),
        )
        .await
        .unwrap();

        assert_eq!(resp.response.result.unwrap().get(), r#""0xb""#);
        assert_eq!(a.call_count(), 1);
        assert_eq!(b.call_count(), 1);
    }

    #[tokio::test]
    async fn definitive_error_stops_routing() {
        let a = ScriptedUpstream::err("a", UpstreamError::InvalidResponse("garbage".into()));
        let b = ScriptedUpstream::ok("b", r#"{"jsonrpc":"2.0","id":1,"result":"0xb"}"#);

        let result = route(
            vec_of(&[a.clone(), b.clone()]),
            Box::new(AlwaysQuorum::new()),
            &dummy_request(),
        )
        .await;

        assert!(matches!(result, Err(UpstreamError::InvalidResponse(_))));
        assert_eq!(a.call_count(), 1);
        assert_eq!(b.call_count(), 0);
    }

    #[tokio::test]
    async fn surfaces_deferred_error_when_all_fail() {
        let a = ScriptedUpstream::err("a", UpstreamError::HttpStatus(429));
        let b = ScriptedUpstream::err("b", UpstreamError::HttpStatus(503));

        let err = route(
            vec_of(&[a.clone(), b.clone()]),
            Box::new(AlwaysQuorum::new()),
            &dummy_request(),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, UpstreamError::HttpStatus(_)));
    }

    #[tokio::test]
    async fn returns_transport_error_when_no_candidates() {
        let err = route(vec![], Box::new(AlwaysQuorum::new()), &dummy_request())
            .await
            .unwrap_err();
        assert!(matches!(err, UpstreamError::Transport(_)));
    }
}
