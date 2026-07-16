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

//! Wrapper that records connection metrics for every call to an upstream
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
use crate::metrics::{self, UpstreamProtocol};
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

#[async_trait::async_trait]
impl RpcUpstream for MeteredUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        metrics::upstream_enqueued(self.upstream.as_str(), &self.chain);
        let start = std::time::Instant::now();
        let result = self.inner.call(request).await;
        metrics::upstream_finished(self.upstream.as_str(), &self.chain);
        match &result {
            Ok(response) => {
                metrics::upstream_call(
                    self.protocol,
                    self.upstream.as_str(),
                    &self.chain,
                    start.elapsed(),
                );
                // Like the legacy `RpcMetrics.processResponseSize`, the size is
                // of the result payload — an error response records nothing.
                if let Some(payload) = &response.result {
                    metrics::upstream_response_size(
                        self.protocol,
                        self.upstream.as_str(),
                        &self.chain,
                        payload.get().len(),
                    );
                }
            }
            Err(_) => {
                metrics::upstream_fail(self.protocol, self.upstream.as_str(), &self.chain);
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
