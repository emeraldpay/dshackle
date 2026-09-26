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

//! Shared handling of non-200 HTTP responses from JSON-RPC upstreams (Ethereum
//! and Bitcoin), mirroring the legacy `JsonRpcHttpClient.asJsonRpcResponse` plus
//! `DefaultUpstream.watchHttpCodes`:
//!
//! - Forward the provider's own JSON-RPC error message when the body carries one
//!   (issue #251; Bitcoin nodes answer 500 with a real "Already Spent" error).
//! - Keep the status, so the quorum can tell a retryable refusal (429/401/502–504)
//!   from a definitive answer.
//!
//! Pausing the upstream on such a status is not done here but by the
//! [`OverloadGuard`](super::overload::OverloadGuard) above both transports:
//! the provider refuses the whole upstream, not only its HTTP endpoint.

use crate::jsonrpc::JsonRpcResponse;
use crate::upstream::id::UpstreamId;
use crate::upstream::traits::{UpstreamError, sanitize_error_body};

/// Map a non-200 response into an [`UpstreamError`], forwarding the provider's
/// own error message when the body is a JSON-RPC error.
pub fn classify_non_200(id: &UpstreamId, status: u16, body: &str) -> UpstreamError {
    match rpc_error_message(body) {
        Some(message) => {
            tracing::debug!(upstream = %id, status, %message, "upstream returned a JSON-RPC error with non-200 status");
            UpstreamError::Rejected { status, message }
        }
        None => {
            let sanitized = sanitize_error_body(body);
            tracing::debug!(upstream = %id, status, body = %sanitized, "HTTP non-200 response");
            UpstreamError::HttpStatus(status)
        }
    }
}

/// Extract `error.message` from a JSON-RPC error body, if the body is a
/// well-formed JSON-RPC error response.
fn rpc_error_message(body: &str) -> Option<String> {
    serde_json::from_str::<JsonRpcResponse>(body)
        .ok()?
        .error
        .map(|e| e.message)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ERROR_BODY: &str =
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"too many request"}}"#;

    #[test]
    fn forwards_body_message_on_429() {
        let err = classify_non_200(&"up-u".parse().unwrap(), 429, ERROR_BODY);
        assert!(
            matches!(&err, UpstreamError::Rejected { status: 429, message } if message == "too many request")
        );
    }

    #[test]
    fn forwards_body_message_on_500() {
        // A Bitcoin node answering 500 with a real error is a definitive answer.
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Already Spent"}}"#;
        let err = classify_non_200(&"up-u".parse().unwrap(), 500, body);
        assert!(
            matches!(&err, UpstreamError::Rejected { status: 500, message } if message == "Already Spent")
        );
    }

    #[test]
    fn falls_back_to_status_without_json_body() {
        let err = classify_non_200(&"up-u".parse().unwrap(), 502, "<html>Bad Gateway</html>");
        assert!(matches!(err, UpstreamError::HttpStatus(502)));
    }
}
