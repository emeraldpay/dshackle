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

//! Core upstream abstraction. Every upstream type (Ethereum HTTP, WebSocket,
//! Bitcoin, Dshackle gRPC) will implement `RpcUpstream` so the call pipeline
//! can work with them uniformly.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::state::UpstreamState;
use std::sync::Arc;

/// An upstream that can execute JSON-RPC calls.
#[async_trait::async_trait]
pub trait RpcUpstream: Send + Sync {
    /// Execute a single JSON-RPC request against this upstream.
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError>;

    /// Unique identifier for this upstream (from config).
    fn id(&self) -> &str;

    /// Current availability status of this upstream.
    fn availability(&self) -> UpstreamAvailability;

    /// Access the head tracker for this upstream.
    fn head(&self) -> &dyn Head;

    /// How many blocks this upstream lags behind the best known head.
    /// Returns `None` when lag is unknown or not yet measured.
    fn lag(&self) -> Option<u64>;

    /// Shared mutable state (lag, derived availability) updated by the status tracker.
    fn state(&self) -> &Arc<UpstreamState>;

    /// Whether this upstream will accept the given method.
    ///
    /// Used by `Multistream` to skip upstreams up front instead of discovering
    /// the mismatch via a `MethodNotAllowed` error. The default returns `true`
    /// because most transports don't know their allowed set; the
    /// `MethodFilter` and `HardcodedMethods` wrappers override it to enforce
    /// per-upstream method configs.
    fn allows_method(&self, _method: &RpcMethod) -> bool {
        true
    }
}

/// Errors that can occur when communicating with an upstream.
#[derive(Clone, Debug)]
pub enum UpstreamError {
    /// HTTP transport failure (connection refused, timeout, DNS, etc.).
    Transport(String),
    /// The upstream returned a non-200 HTTP status.
    HttpStatus(u16),
    /// The response body could not be parsed as valid JSON-RPC.
    InvalidResponse(String),
    /// The requested RPC method is not supported by this upstream.
    MethodNotAllowed(String),
}

impl std::fmt::Display for UpstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpstreamError::Transport(msg) => write!(f, "transport error: {msg}"),
            UpstreamError::HttpStatus(code) => write!(f, "upstream returned HTTP {code}"),
            UpstreamError::InvalidResponse(msg) => write!(f, "invalid response: {msg}"),
            UpstreamError::MethodNotAllowed(method) => write!(f, "method not allowed: {method}"),
        }
    }
}

impl std::error::Error for UpstreamError {}

/// Sanitize an HTTP error response body for safe logging.
///
/// Keeps only printable ASCII (0x20..=0x7E) and truncates to 1024 characters.
pub fn sanitize_error_body(body: &str) -> String {
    body.chars()
        .filter(|c| ('\x20'..='\x7E').contains(c))
        .take(1024)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_keeps_printable_ascii() {
        assert_eq!(sanitize_error_body("hello world"), "hello world");
    }

    #[test]
    fn sanitize_strips_control_chars() {
        assert_eq!(sanitize_error_body("error\x00\x01\x02msg"), "errormsg");
    }

    #[test]
    fn sanitize_strips_newlines_and_tabs() {
        assert_eq!(sanitize_error_body("line1\nline2\ttab"), "line1line2tab");
    }

    #[test]
    fn sanitize_strips_non_ascii() {
        assert_eq!(sanitize_error_body("café résumé"), "caf rsum");
    }

    #[test]
    fn sanitize_truncates_to_1024() {
        let long = "a".repeat(2000);
        assert_eq!(sanitize_error_body(&long).len(), 1024);
    }

    #[test]
    fn sanitize_empty_input() {
        assert_eq!(sanitize_error_body(""), "");
    }

    #[test]
    fn sanitize_preserves_json() {
        let json = r#"{"error":"Internal Server Error","code":500}"#;
        assert_eq!(sanitize_error_body(json), json);
    }
}
