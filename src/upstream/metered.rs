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

//! Wrapper that records request metrics for every call to an upstream
//! transport. The equivalent of the legacy `RpcMetrics` attached to each
//! JSON-RPC/WS/gRPC client.
//!
//! It's installed directly above the transport — below the cache and method
//! filter — so only requests that actually reach the upstream are measured;
//! answers served from the cache or rejected locally don't count, matching
//! where the legacy clients recorded them.

use crate::blockchain::TargetBlockchain;
use crate::config::upstreams::UpstreamRole;
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::metrics::{self, RequestStatus, UpstreamProtocol};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::id::UpstreamId;
use crate::upstream::label::UpstreamLabels;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{Capability, RpcUpstream, UpstreamError};
use std::sync::Arc;

/// Decorates an upstream transport with call metrics, delegating all request
/// handling to the inner upstream unchanged.
pub struct MeteredUpstream {
    inner: Arc<dyn RpcUpstream>,
    protocol: UpstreamProtocol,
    /// The `upstream` metric label: the configured upstream id. Passed
    /// explicitly because an inner id may carry extra qualifiers (a remote
    /// Dshackle uses `id_chain`), while the legacy metrics were always tagged
    /// with the plain id.
    upstream: UpstreamId,
    chain: TargetBlockchain,
}

impl MeteredUpstream {
    pub fn new(
        inner: Arc<dyn RpcUpstream>,
        protocol: UpstreamProtocol,
        upstream: UpstreamId,
        chain: TargetBlockchain,
    ) -> Self {
        metrics::upstream_created(protocol, upstream.as_str(), &chain);
        Self {
            inner,
            protocol,
            upstream,
            chain,
        }
    }
}

/// A request sent through a [`MeteredUpstream`], settled in the metrics when
/// it's dropped. The call's future may be dropped before it completes, e.g.
/// by a validation timeout or a client that left; that request still counts,
/// as `failed`, and leaves the in-flight gauge, which would otherwise only
/// grow.
struct SentRequest<'a> {
    metered: &'a MeteredUpstream,
    status: RequestStatus,
}

impl<'a> SentRequest<'a> {
    fn new(metered: &'a MeteredUpstream) -> Self {
        metrics::upstream_sent(metered.protocol, metered.upstream.as_str(), &metered.chain);
        Self {
            metered,
            status: RequestStatus::Failed,
        }
    }
}

impl Drop for SentRequest<'_> {
    fn drop(&mut self) {
        metrics::upstream_finished(
            self.metered.protocol,
            self.metered.upstream.as_str(),
            &self.metered.chain,
            self.status,
        );
    }
}

#[async_trait::async_trait]
impl RpcUpstream for MeteredUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        let upstream = self.upstream.as_str();
        let mut sent = SentRequest::new(self);
        let start = std::time::Instant::now();
        let result = self.inner.call(request).await;
        let status = RequestStatus::from(result.as_ref());
        sent.status = status;
        drop(sent);
        // An `error` is an answer too, also when it comes as an `Err`, e.g.
        // Bitcoin's 500 "Already Spent"; a refusal is `failed`, not answered.
        if matches!(status, RequestStatus::Success | RequestStatus::Error) {
            metrics::upstream_answered(self.protocol, upstream, &self.chain, start.elapsed());
        }
        if let Ok(response) = &result {
            // Like the legacy `RpcMetrics.processResponseSize`, the size is
            // of the result payload — an error response records nothing.
            if let Some(payload) = &response.result {
                metrics::upstream_response_size(
                    self.protocol,
                    upstream,
                    &self.chain,
                    payload.get().len(),
                );
            }
        }
        result
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

    fn label_sets(&self) -> &[UpstreamLabels] {
        self.inner.label_sets()
    }

    fn role(&self) -> UpstreamRole {
        self.inner.role()
    }

    fn capabilities(&self) -> Vec<Capability> {
        self.inner.capabilities()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::JsonRpcError;
    use crate::metrics::tests::{enable, scrape};
    use crate::upstream::head::NoHead;
    use std::time::Duration;

    fn rpc_error(message: &str) -> JsonRpcError {
        JsonRpcError {
            code: -32000,
            message: message.into(),
            data: None,
        }
    }

    /// An upstream that never answers.
    struct HangingUpstream {
        state: Arc<UpstreamState>,
    }

    #[async_trait::async_trait]
    impl RpcUpstream for HangingUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            std::future::pending().await
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

    #[tokio::test(start_paused = true)]
    async fn call_dropped_midway_counts_as_failed() {
        enable();
        let metered = MeteredUpstream::new(
            Arc::new(HangingUpstream {
                state: Arc::new(UpstreamState::new()),
            }),
            UpstreamProtocol::HttpJsonRpc,
            "metered-hanging".parse().unwrap(),
            "ethereum".parse().unwrap(),
        );
        let request = JsonRpcRequest::new(1, "eth_blockNumber".into(), serde_json::json!([]));

        // Like a validation probe giving up on a hung upstream.
        let timed_out = tokio::time::timeout(Duration::from_secs(5), metered.call(&request)).await;

        assert!(timed_out.is_err());
        let out = scrape();
        assert!(out.contains(
            r#"dshackle_upstream_requests_in_flight{chain="ETH",protocol="http-json-rpc",upstream="metered-hanging"} 0"#
        ));
        assert!(out.contains(
            r#"dshackle_upstream_requests_total{chain="ETH",protocol="http-json-rpc",status="failed",upstream="metered-hanging"} 1"#
        ));
    }

    /// An upstream that answers every call with an HTTP 500 and `message`.
    struct RejectingUpstream {
        message: &'static str,
        state: Arc<UpstreamState>,
    }

    #[async_trait::async_trait]
    impl RpcUpstream for RejectingUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            Err(UpstreamError::Rejected {
                status: 500,
                error: rpc_error(self.message),
            })
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

    /// Calls an upstream answering 500 with `message` once, through a
    /// [`MeteredUpstream`] labelled `upstream`.
    async fn call_rejecting(upstream: &str, message: &'static str) {
        let metered = MeteredUpstream::new(
            Arc::new(RejectingUpstream {
                message,
                state: Arc::new(UpstreamState::new()),
            }),
            UpstreamProtocol::HttpJsonRpc,
            upstream.parse().unwrap(),
            "bitcoin".parse().unwrap(),
        );
        let request = JsonRpcRequest::new(1, "sendrawtransaction".into(), serde_json::json!([]));
        assert!(metered.call(&request).await.is_err());
    }

    #[tokio::test]
    async fn definitive_answer_behind_http_error_is_timed() {
        enable();
        call_rejecting("metered-already-spent", "Already Spent").await;

        let out = scrape();
        assert!(out.contains(
            r#"dshackle_upstream_requests_total{chain="BTC",protocol="http-json-rpc",status="error",upstream="metered-already-spent"} 1"#
        ));
        assert!(out.contains(
            r#"dshackle_upstream_request_duration_seconds_count{chain="BTC",protocol="http-json-rpc",upstream="metered-already-spent"} 1"#
        ));
    }

    #[tokio::test]
    async fn overload_behind_http_error_is_not_timed() {
        enable();
        call_rejecting("metered-overloaded", "server overloaded, retry later").await;

        let out = scrape();
        assert!(out.contains(
            r#"dshackle_upstream_requests_total{chain="BTC",protocol="http-json-rpc",status="failed",upstream="metered-overloaded"} 1"#
        ));
        // The series exists from the upstream's creation, with nothing in it.
        assert!(out.contains(
            r#"dshackle_upstream_request_duration_seconds_count{chain="BTC",protocol="http-json-rpc",upstream="metered-overloaded"} 0"#
        ));
    }
}
