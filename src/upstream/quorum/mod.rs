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

//! Quorum strategies for upstream calls.
//!
//! A `CallQuorum` is a stateful policy that decides when enough upstreams have
//! responded to consider the request resolved (or definitively failed). The
//! [`UpstreamRouter`](super::router) feeds it the outcome of each call, asks
//! whether to keep going, and finally extracts the resolved response.
//!
//! For now only [`AlwaysQuorum`] is implemented, which accepts the first
//! response from any upstream. Future strategies (`NotLagging`, `Broadcast`,
//! `NonEmpty`, `Nonce`) will plug into the same trait.

pub mod always;
pub mod broadcast;
pub mod non_empty;
pub mod nonce;
pub mod not_lagging;

pub use always::AlwaysQuorum;
pub use broadcast::BroadcastQuorum;
pub use non_empty::NonEmptyQuorum;
pub use nonce::NonceQuorum;
pub use not_lagging::NotLaggingQuorum;

use crate::jsonrpc::{JsonRpcResponse, RpcMethod};
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use serde_json::value::RawValue;

/// Picks a `CallQuorum` strategy for each RPC method, and reports which
/// methods the backing configuration considers callable or hardcoded.
///
/// Concrete implementations live next to the per-chain method tables in
/// [`crate::upstream::methods`], since the mapping is fundamentally a
/// per-method configuration. The `is_callable`/`is_hardcoded` methods let an
/// aggregator (e.g. `AggregatedMethods`) pick a delegate that actually
/// supports a method, mirroring the legacy `AggregatedCallMethods`.
pub trait QuorumFactory: Send + Sync {
    fn quorum_for(&self, method: &RpcMethod) -> Box<dyn CallQuorum>;

    /// Whether the method may be forwarded to an upstream through this config.
    /// Defaults to `true` so backends without an explicit method list act as
    /// pass-through (e.g. Dshackle gRPC remotes).
    fn is_callable(&self, _method: &RpcMethod) -> bool {
        true
    }

    /// Whether the method is answered from a hardcoded response rather than
    /// forwarded to the upstream. Default derives from
    /// [`hardcoded_response`](Self::hardcoded_response) so implementors only
    /// override one.
    fn is_hardcoded(&self, method: &RpcMethod) -> bool {
        self.hardcoded_response(method).is_some()
    }

    /// The hardcoded response body for a method, if one is configured.
    /// Returning `Some` means the method bypasses the upstream entirely.
    fn hardcoded_response(&self, _method: &RpcMethod) -> Option<&RawValue> {
        None
    }

    /// Every method this configuration can serve — callable plus hardcoded —
    /// as advertised by `Describe` (legacy `CallMethods.getSupportedMethods`).
    /// Defaults to empty for pass-through factories that keep no explicit list
    /// (e.g. Dshackle remotes), so they advertise nothing rather than guessing.
    fn supported_methods(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Decides when a request has gathered enough responses to be considered done.
///
/// The router calls `record_response` / `record_error` for each upstream
/// outcome and consults `is_resolved` / `is_failed` to know when to stop.
pub trait CallQuorum: Send {
    /// Tells the quorum how many upstreams are available in total. Some
    /// strategies clamp their requirements (e.g. broadcast to N) against this.
    fn set_total_upstreams(&mut self, total: usize);

    /// Record a successful upstream response. A response carrying a JSON-RPC
    /// error object is still a valid answer from the upstream and is treated
    /// as a successful outcome here — the caller forwards it to the client.
    fn record_response(&mut self, response: JsonRpcResponse, upstream: &dyn RpcUpstream);

    /// Record a transport-level or protocol-level failure for an upstream.
    fn record_error(&mut self, error: &UpstreamError, upstream: &dyn RpcUpstream);

    /// Whether the quorum has gathered enough data to satisfy the request.
    fn is_resolved(&self) -> bool;

    /// Whether the quorum has definitively failed and no further attempts
    /// can salvage it.
    fn is_failed(&self) -> bool;

    /// Called once when the router has run out of upstreams to try. This is
    /// the last chance for a quorum to promote a tentative state into a final
    /// one (e.g. surface a connection error that was previously held back).
    fn close(&mut self) {}

    /// Consume the accumulated state and return the final outcome.
    fn take_outcome(&mut self) -> QuorumOutcome;

    /// Id of the upstream whose response `take_outcome` resolved with, needed
    /// to attribute the response (e.g. for signing). Only meaningful after
    /// `take_outcome` returned `Resolved`.
    fn resolved_by(&self) -> Option<&str> {
        None
    }

    /// Hint about which upstreams the router should consider. Quorums that
    /// require fresh data (e.g. `NotLagging`) override this to filter out
    /// stale upstreams up front; the default is "any available upstream".
    fn selector(&self) -> SelectorHint {
        SelectorHint::Available
    }
}

/// Tells the [`Multistream`](crate::upstream::Multistream) how to pre-filter
/// candidate upstreams before the router starts calling them.
#[derive(Clone, Copy, Debug)]
pub enum SelectorHint {
    /// Any upstream that's currently `Ok`, `Lagging`, or `Immature`.
    Available,
    /// Only upstreams whose lag is `<= max_lag` blocks behind the best head.
    NotLagging { max_lag: u64 },
}

/// Final outcome of a quorum-driven call.
pub enum QuorumOutcome {
    /// At least one upstream produced a response that the quorum accepted.
    Resolved(JsonRpcResponse),
    /// Every attempted upstream failed in a way the quorum considered fatal.
    Failed(UpstreamError),
    /// No upstream was reachable or the quorum never reached a decision.
    Empty,
}

/// Whether an error indicates the upstream is temporarily unavailable and the
/// router should try another one rather than fail the request.
///
/// Mirrors the legacy `CallQuorum.isConnectionUnavailable` rules: HTTP 401,
/// 429, and 502–504 are commonly returned by overloaded or auth-rejecting
/// proxies (e.g. Infura), and any transport-level failure (connect, timeout,
/// DNS) qualifies. A `MethodNotAllowed` error is also treated as skippable
/// because a peer upstream may support the method.
pub fn is_connection_unavailable(err: &UpstreamError) -> bool {
    match err {
        UpstreamError::Transport(_) => true,
        UpstreamError::HttpStatus(code) => is_unavailable_status(*code),
        UpstreamError::Rejected { status, .. } => is_unavailable_status(*status),
        UpstreamError::MethodNotAllowed(_) => true,
        UpstreamError::InvalidResponse(_) => false,
    }
}

/// Whether an HTTP status marks the upstream as temporarily unavailable — the
/// call may be retried on another upstream, and the provider should be parked.
///
/// 401 (Unauthorized), 429 (Too Many Requests), and 502–504 are commonly
/// returned by overloaded or auth-rejecting proxies (e.g. Infura). A 500 with a
/// JSON-RPC body, by contrast, is a definitive answer about the request itself.
pub fn is_unavailable_status(code: u16) -> bool {
    code == 401 || code == 429 || (502..=504).contains(&code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_error_is_connection_unavailable() {
        let err = UpstreamError::Transport("connection refused".into());
        assert!(is_connection_unavailable(&err));
    }

    #[test]
    fn http_429_is_connection_unavailable() {
        assert!(is_connection_unavailable(&UpstreamError::HttpStatus(429)));
    }

    #[test]
    fn http_401_is_connection_unavailable() {
        assert!(is_connection_unavailable(&UpstreamError::HttpStatus(401)));
    }

    #[test]
    fn http_502_503_504_are_connection_unavailable() {
        assert!(is_connection_unavailable(&UpstreamError::HttpStatus(502)));
        assert!(is_connection_unavailable(&UpstreamError::HttpStatus(503)));
        assert!(is_connection_unavailable(&UpstreamError::HttpStatus(504)));
    }

    #[test]
    fn http_400_and_500_are_definitive() {
        assert!(!is_connection_unavailable(&UpstreamError::HttpStatus(400)));
        assert!(!is_connection_unavailable(&UpstreamError::HttpStatus(500)));
    }

    #[test]
    fn http_404_is_definitive() {
        // Bitcoin Core returns 404 in normal operation for some unknown methods;
        // see legacy `CallQuorumTest` "Understand errors for available connection".
        assert!(!is_connection_unavailable(&UpstreamError::HttpStatus(404)));
    }

    #[test]
    fn method_not_allowed_is_skippable() {
        let err = UpstreamError::MethodNotAllowed("debug_traceTransaction".into());
        assert!(is_connection_unavailable(&err));
    }

    #[test]
    fn invalid_response_is_definitive() {
        let err = UpstreamError::InvalidResponse("bad json".into());
        assert!(!is_connection_unavailable(&err));
    }
}
