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

//! How a call to an upstream ended, as the `status` of the request metrics.

use crate::jsonrpc::JsonRpcResponse;
use crate::metrics::RequestStatus;
use crate::upstream::overload::Refusal;
use crate::upstream::traits::UpstreamError;

/// How a call ended, for the request metrics of both the API and the
/// upstream connections.
///
/// A refusal counts as `failed` however it's delivered: the metered transport
/// sits below the [`OverloadGuard`](super::overload::OverloadGuard), so it
/// sees a provider outage still as a JSON-RPC error or an HTTP error, not yet
/// as [`UpstreamError::Overloaded`]. Using the guard's own [`Refusal`] check
/// keeps the two from disagreeing about what a refusal is.
impl From<Result<&JsonRpcResponse, &UpstreamError>> for RequestStatus {
    fn from(result: Result<&JsonRpcResponse, &UpstreamError>) -> Self {
        match result {
            Ok(response) => match &response.error {
                None => RequestStatus::Success,
                Some(error) if error.pause_reason().is_some() => RequestStatus::Failed,
                Some(_) => RequestStatus::Error,
            },
            Err(err) if err.pause_reason().is_some() => RequestStatus::Failed,
            Err(UpstreamError::MethodNotAllowed(_)) => RequestStatus::Error,
            // Not a refusal, so it's the node's own answer about the request,
            // e.g. Bitcoin's 500 "Already Spent".
            Err(UpstreamError::Rejected { .. }) => RequestStatus::Error,
            // Our own limits, or a remote Dshackle whose upstreams are all at
            // theirs.
            Err(UpstreamError::LimitReached) => RequestStatus::RateLimited,
            Err(_) => RequestStatus::Failed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::JsonRpcError;

    fn rpc_error(message: &str) -> JsonRpcError {
        JsonRpcError {
            code: -32000,
            message: message.into(),
            data: None,
        }
    }

    fn error_response(code: i64, message: &str) -> JsonRpcResponse {
        serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": code, "message": message},
        }))
        .unwrap()
    }

    #[test]
    fn node_rejecting_the_request_is_an_error() {
        let rejected = UpstreamError::Rejected {
            status: 500,
            error: rpc_error("Already Spent"),
        };
        assert_eq!(RequestStatus::from(Err(&rejected)), RequestStatus::Error);
    }

    #[test]
    fn struggling_provider_is_a_failure() {
        let rejected = UpstreamError::Rejected {
            status: 429,
            error: rpc_error("Too Many Requests"),
        };
        assert_eq!(RequestStatus::from(Err(&rejected)), RequestStatus::Failed);
    }

    #[test]
    fn answer_about_the_request_is_an_error() {
        let response = error_response(-32000, "execution reverted");
        assert_eq!(RequestStatus::from(Ok(&response)), RequestStatus::Error);
    }

    #[test]
    fn refusal_as_json_rpc_error_is_a_failure() {
        // Alchemy's rate limit, and Erigon busy with an internal job.
        for response in [
            error_response(429, "exceeded its compute units per second capacity"),
            error_response(-32005, "server overloaded, retry later"),
        ] {
            assert_eq!(RequestStatus::from(Ok(&response)), RequestStatus::Failed);
        }
    }

    #[test]
    fn overload_behind_a_500_is_a_failure() {
        let rejected = UpstreamError::Rejected {
            status: 500,
            error: rpc_error("server overloaded, retry later"),
        };
        assert_eq!(RequestStatus::from(Err(&rejected)), RequestStatus::Failed);
    }

    #[test]
    fn limit_refusal_is_rate_limited() {
        assert_eq!(
            RequestStatus::from(Err(&UpstreamError::LimitReached)),
            RequestStatus::RateLimited
        );
    }
}
