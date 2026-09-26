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

//! Pauses an upstream that refuses calls.
//!
//! An upstream refuses calls in a few ways, all meaning "not now, try
//! elsewhere":
//!
//! - HTTP 429 (provider rate limit), 401, or 502–504 on the HTTP transport.
//! - A JSON-RPC error with the same meaning, with no HTTP-level signal: over
//!   WebSocket there's no status at all. Alchemy sends code `429` ("exceeded
//!   its compute units per second capacity"), Infura its "request rate"
//!   messages, and Erigon busy with an internal job answers
//!   "server overloaded, retry later".
//!
//! Any of them pauses the whole upstream, not only the transport that got it:
//! the WS and HTTP endpoints of one upstream are the same node or the same
//! provider account. The pause backs off while the refusals continue (see
//! [`PauseReason`]), and the call itself fails over to another upstream.
//!
//! Erigon's overload is recognized by the message, not the code: `-32005`
//! also means a request-specific limit on some providers (e.g. "query
//! returned more than 10000 results"), and some Erigon versions answer
//! overload with `-32000`.

use crate::jsonrpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::id::UpstreamId;
use crate::upstream::pause::PauseReason;
use crate::upstream::quorum::is_unavailable_status;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// Message prefixes of known "node overloaded" errors.
const OVERLOAD_MESSAGES: &[&str] = &[
    // Erigon, when its RPC admission limit or DB read-transaction limit is full.
    "server overloaded",
];

/// Message prefixes of known rate-limit errors that come without a 429 code.
const RATE_LIMIT_MESSAGES: &[&str] = &[
    // Infura, with code -32005, which alone is ambiguous (see the module doc).
    "daily request count exceeded",
    "project id request rate exceeded",
];

/// JSON-RPC error code some providers (Alchemy) reuse from HTTP 429.
const RATE_LIMIT_CODE: i64 = 429;

/// Wraps the transport of one upstream and pauses that upstream when it
/// refuses a call. A refusal delivered as a JSON-RPC error is turned into an
/// [`UpstreamError::Overloaded`], so the router moves on to another upstream
/// instead of handing the refusal to the client as the answer.
///
/// Must sit above the WS/HTTP switch: its `state` is the upstream-level one
/// routing reads, while each transport only has its own.
pub struct OverloadGuard {
    inner: Arc<dyn RpcUpstream>,
}

impl OverloadGuard {
    pub fn new(inner: Arc<dyn RpcUpstream>) -> Self {
        Self { inner }
    }

    fn pause(&self, reason: PauseReason, message: &str) {
        if let Some(cooldown) = self.inner.state().pause(reason) {
            tracing::warn!(
                upstream = %self.inner.id(),
                %message,
                "upstream refuses calls ({reason:?}), pausing it for {}ms",
                cooldown.as_millis()
            );
        }
    }
}

/// An upstream's reply that may say it refuses calls for now.
trait Refusal {
    fn pause_reason(&self) -> Option<PauseReason>;
}

fn starts_with_any(message: &str, prefixes: &[&str]) -> bool {
    let message = message.to_ascii_lowercase();
    prefixes.iter().any(|p| message.starts_with(p))
}

impl Refusal for JsonRpcError {
    fn pause_reason(&self) -> Option<PauseReason> {
        if starts_with_any(&self.message, OVERLOAD_MESSAGES) {
            Some(PauseReason::Overloaded)
        } else if self.code == RATE_LIMIT_CODE
            || starts_with_any(&self.message, RATE_LIMIT_MESSAGES)
        {
            Some(PauseReason::RateLimited)
        } else {
            None
        }
    }
}

impl Refusal for UpstreamError {
    fn pause_reason(&self) -> Option<PauseReason> {
        match self {
            // Erigon behind HTTP answers overload with a 503.
            UpstreamError::Rejected { message, .. }
                if starts_with_any(message, OVERLOAD_MESSAGES) =>
            {
                Some(PauseReason::Overloaded)
            }
            UpstreamError::Rejected { status, .. } | UpstreamError::HttpStatus(status)
                if is_unavailable_status(*status) =>
            {
                Some(PauseReason::RateLimited)
            }
            _ => None,
        }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for OverloadGuard {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        match self.inner.call(request).await {
            Ok(response) => {
                if let Some(error) = &response.error
                    && let Some(reason) = error.pause_reason()
                {
                    self.pause(reason, &error.message);
                    return Err(UpstreamError::Overloaded(error.message.clone()));
                }
                self.inner.state().record_answer();
                Ok(response)
            }
            Err(err) => {
                let Some(reason) = err.pause_reason() else {
                    return Err(err);
                };
                self.pause(reason, &err.to_string());
                match err {
                    // The status of an overload reply depends on what's in
                    // front of the node (e.g. a proxy answering 500), and one
                    // outside 401/429/502–504 would read as a definitive
                    // answer. The message is what says to try elsewhere.
                    UpstreamError::Rejected { message, .. }
                        if reason == PauseReason::Overloaded =>
                    {
                        Err(UpstreamError::Overloaded(message))
                    }
                    other => Err(other),
                }
            }
        }
    }

    fn id(&self) -> &UpstreamId {
        self.inner.id()
    }

    fn availability(&self) -> UpstreamAvailability {
        self.inner.availability()
    }

    fn head(&self) -> &dyn Head {
        self.inner.head()
    }

    fn lag(&self) -> Option<u64> {
        self.inner.lag()
    }

    fn state(&self) -> &Arc<UpstreamState> {
        self.inner.state()
    }

    fn allows_method(&self, method: &RpcMethod) -> bool {
        self.inner.allows_method(method)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::head::NoHead;
    use crate::upstream::quorum::is_connection_unavailable;

    struct ScriptedUpstream {
        outcome: Result<String, UpstreamError>,
        state: Arc<UpstreamState>,
    }

    impl ScriptedUpstream {
        fn new(outcome: Result<String, UpstreamError>) -> Arc<Self> {
            Arc::new(Self {
                outcome,
                state: Arc::new(UpstreamState::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for ScriptedUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            self.outcome
                .clone()
                .map(|body| serde_json::from_str(&body).unwrap())
        }
        fn id(&self) -> &UpstreamId {
            crate::upstream::id::stub_id()
        }
        fn availability(&self) -> UpstreamAvailability {
            self.state.availability()
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

    fn request() -> JsonRpcRequest {
        JsonRpcRequest::new(1, "eth_getTransactionByHash".into(), serde_json::json!([]))
    }

    fn error_body(code: i64, message: &str) -> String {
        format!(r#"{{"jsonrpc":"2.0","id":1,"error":{{"code":{code},"message":"{message}"}}}}"#)
    }

    #[tokio::test]
    async fn overload_error_pauses_upstream_and_fails_the_call() {
        // The code Erigon sends varies between versions; the message decides.
        for code in [-32000, -32005] {
            let inner =
                ScriptedUpstream::new(Ok(error_body(code, "server overloaded, retry later")));
            let guard = OverloadGuard::new(inner);

            let err = guard.call(&request()).await.unwrap_err();

            assert!(
                matches!(&err, UpstreamError::Overloaded(m) if m == "server overloaded, retry later")
            );
            assert_eq!(guard.availability(), UpstreamAvailability::Unavailable);
        }
    }

    #[tokio::test]
    async fn rate_limit_as_rpc_error_pauses_upstream() {
        // How Alchemy refuses over WebSocket, with no HTTP status to go by.
        let inner = ScriptedUpstream::new(Ok(error_body(
            429,
            "Your app has exceeded its compute units per second capacity",
        )));
        let guard = OverloadGuard::new(inner);

        let err = guard.call(&request()).await.unwrap_err();

        assert!(matches!(err, UpstreamError::Overloaded(_)));
        assert_eq!(guard.availability(), UpstreamAvailability::Unavailable);
    }

    #[tokio::test]
    async fn infura_rate_limit_message_pauses_upstream() {
        let inner =
            ScriptedUpstream::new(Ok(error_body(-32005, "project ID request rate exceeded")));
        let guard = OverloadGuard::new(inner);

        assert!(guard.call(&request()).await.is_err());
        assert_eq!(guard.availability(), UpstreamAvailability::Unavailable);
    }

    #[tokio::test]
    async fn http_429_pauses_upstream() {
        let inner = ScriptedUpstream::new(Err(UpstreamError::Rejected {
            status: 429,
            message: "Your app has exceeded its compute units per second capacity".into(),
        }));
        let guard = OverloadGuard::new(inner);

        let err = guard.call(&request()).await.unwrap_err();

        assert!(matches!(err, UpstreamError::Rejected { status: 429, .. }));
        assert_eq!(guard.availability(), UpstreamAvailability::Unavailable);
    }

    #[tokio::test]
    async fn overload_behind_any_http_status_pauses_and_fails_over() {
        // A 500 alone would be a definitive answer; the message overrides it.
        for status in [500, 503] {
            let inner = ScriptedUpstream::new(Err(UpstreamError::Rejected {
                status,
                message: "server overloaded, retry later".into(),
            }));
            let guard = OverloadGuard::new(inner);

            let err = guard.call(&request()).await.unwrap_err();

            assert!(
                matches!(&err, UpstreamError::Overloaded(m) if m == "server overloaded, retry later")
            );
            assert!(is_connection_unavailable(&err));
            assert_eq!(guard.availability(), UpstreamAvailability::Unavailable);
        }
    }

    #[tokio::test]
    async fn definitive_http_error_does_not_pause() {
        let inner = ScriptedUpstream::new(Err(UpstreamError::Rejected {
            status: 500,
            message: "Already Spent".into(),
        }));
        let guard = OverloadGuard::new(inner);

        assert!(guard.call(&request()).await.is_err());
        assert_eq!(guard.availability(), UpstreamAvailability::Ok);
    }

    #[tokio::test]
    async fn other_rpc_errors_pass_through() {
        // Same code as Infura's rate limit, but about the request itself.
        let inner = ScriptedUpstream::new(Ok(error_body(
            -32005,
            "query returned more than 10000 results",
        )));
        let guard = OverloadGuard::new(inner);

        let response = guard.call(&request()).await.unwrap();

        assert!(response.error.is_some());
        assert_eq!(guard.availability(), UpstreamAvailability::Ok);
    }

    #[tokio::test]
    async fn results_pass_through() {
        let inner =
            ScriptedUpstream::new(Ok(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#.to_string()));
        let guard = OverloadGuard::new(inner);

        let response = guard.call(&request()).await.unwrap();

        assert_eq!(response.result.unwrap().get(), r#""0x1""#);
        assert_eq!(guard.availability(), UpstreamAvailability::Ok);
    }
}
