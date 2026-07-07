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
//! - Park the upstream for a cooldown when the status marks it temporarily
//!   unavailable (429/401/502–504), so routing stops selecting it.

use crate::jsonrpc::JsonRpcResponse;
use crate::upstream::quorum::is_unavailable_status;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{UpstreamError, sanitize_error_body};
use std::time::Duration;

/// How long to park an upstream after a status that marks it temporarily
/// unavailable, giving a provider's rate-limit window time to reset before we
/// route to it again. Matches the legacy `DefaultUpstream` one-minute pause.
const RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(60);

/// Map a non-200 response into an [`UpstreamError`], parking the upstream when
/// the status marks it temporarily unavailable and forwarding the provider's own
/// error message when the body is a JSON-RPC error.
pub fn classify_non_200(
    id: &str,
    state: &UpstreamState,
    status: u16,
    body: &str,
) -> UpstreamError {
    if is_unavailable_status(status) {
        state.set_rate_limited(RATE_LIMIT_COOLDOWN);
    }
    match rpc_error_message(body) {
        Some(message) => {
            tracing::debug!(upstream = id, status, %message, "upstream returned a JSON-RPC error with non-200 status");
            UpstreamError::Rejected { status, message }
        }
        None => {
            let sanitized = sanitize_error_body(body);
            tracing::debug!(upstream = id, status, body = %sanitized, "HTTP non-200 response");
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
    use crate::upstream::availability::UpstreamAvailability;

    const ERROR_BODY: &str =
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"too many request"}}"#;

    #[test]
    fn forwards_body_message_and_parks_on_429() {
        let state = UpstreamState::new();
        let err = classify_non_200("u", &state, 429, ERROR_BODY);
        assert!(matches!(&err, UpstreamError::Rejected { status: 429, message } if message == "too many request"));
        // A 429 takes the upstream out of rotation.
        assert_eq!(state.availability(), UpstreamAvailability::Unavailable);
    }

    #[test]
    fn forwards_body_message_without_parking_on_500() {
        // A Bitcoin node answering 500 with a real error is a definitive answer,
        // not a reason to park the upstream.
        let state = UpstreamState::new();
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Already Spent"}}"#;
        let err = classify_non_200("u", &state, 500, body);
        assert!(matches!(&err, UpstreamError::Rejected { status: 500, message } if message == "Already Spent"));
        assert_eq!(state.availability(), UpstreamAvailability::Ok);
    }

    #[test]
    fn falls_back_to_status_without_json_body() {
        let state = UpstreamState::new();
        let err = classify_non_200("u", &state, 502, "<html>Bad Gateway</html>");
        assert!(matches!(err, UpstreamError::HttpStatus(502)));
        // 502 still parks the upstream even without a parseable body.
        assert_eq!(state.availability(), UpstreamAvailability::Unavailable);
    }
}
