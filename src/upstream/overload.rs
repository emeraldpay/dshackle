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

//! Parks an upstream whose node says it's overloaded.
//!
//! A node can refuse calls for a while without any HTTP-level signal: Erigon
//! busy with an internal job answers "server overloaded, retry later" as a
//! plain JSON-RPC error (over WebSocket, or HTTP 200). It's the same situation
//! as an HTTP 429, so it gets the same treatment: the upstream leaves rotation
//! for [`RATE_LIMIT_COOLDOWN`] and the call is retried on another upstream.
//!
//! The error code alone can't identify it: `-32005` also means a
//! request-specific limit on some providers (e.g. "query returned more than
//! 10000 results"), and some Erigon versions answer overload with `-32000`.
//! So it's recognized by the message.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::http_error::RATE_LIMIT_COOLDOWN;
use crate::upstream::id::UpstreamId;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// Message prefixes of known "node overloaded" errors.
const OVERLOAD_MESSAGES: &[&str] = &[
    // Erigon, when its RPC admission limit or DB read-transaction limit is full.
    "server overloaded",
];

/// Wraps the transport of one upstream and parks that upstream when the node
/// reports it's overloaded, turning the reply into an
/// [`UpstreamError::Overloaded`] so the router moves on to another upstream.
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

    fn park(&self, message: &str) {
        tracing::warn!(
            upstream = %self.inner.id(),
            %message,
            "upstream is overloaded, pausing it for {}s",
            RATE_LIMIT_COOLDOWN.as_secs()
        );
        self.inner.state().set_rate_limited(RATE_LIMIT_COOLDOWN);
    }
}

fn is_overload_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    OVERLOAD_MESSAGES.iter().any(|m| message.starts_with(m))
}

#[async_trait::async_trait]
impl RpcUpstream for OverloadGuard {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        match self.inner.call(request).await {
            Ok(response) => match &response.error {
                Some(error) if is_overload_message(&error.message) => {
                    self.park(&error.message);
                    Err(UpstreamError::Overloaded(error.message.clone()))
                }
                _ => Ok(response),
            },
            // The same answer behind an HTTP 503 already parked the HTTP
            // transport, but with a WS transport next to it that alone
            // doesn't take the upstream out of rotation.
            Err(UpstreamError::Rejected { message, .. }) if is_overload_message(&message) => {
                self.park(&message);
                Err(UpstreamError::Overloaded(message))
            }
            other => other,
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

    #[tokio::test]
    async fn overload_error_parks_upstream_and_fails_the_call() {
        // The code Erigon sends varies between versions; the message decides.
        for code in [-32000, -32005] {
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"error":{{"code":{code},"message":"server overloaded, retry later"}}}}"#
            );
            let inner = ScriptedUpstream::new(Ok(body));
            let guard = OverloadGuard::new(inner.clone());

            let err = guard.call(&request()).await.unwrap_err();

            assert!(
                matches!(&err, UpstreamError::Overloaded(m) if m == "server overloaded, retry later")
            );
            assert_eq!(guard.availability(), UpstreamAvailability::Unavailable);
        }
    }

    #[tokio::test]
    async fn overload_behind_http_503_parks_upstream() {
        let inner = ScriptedUpstream::new(Err(UpstreamError::Rejected {
            status: 503,
            message: "server overloaded, retry later".into(),
        }));
        let guard = OverloadGuard::new(inner);

        let err = guard.call(&request()).await.unwrap_err();

        assert!(matches!(err, UpstreamError::Overloaded(_)));
        assert_eq!(guard.availability(), UpstreamAvailability::Unavailable);
    }

    #[tokio::test]
    async fn other_rpc_errors_pass_through() {
        // Same code as Infura's rate limit, but about the request itself.
        let inner = ScriptedUpstream::new(Ok(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query returned more than 10000 results"}}"#
                .to_string(),
        ));
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
