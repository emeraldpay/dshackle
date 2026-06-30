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

//! Wrapper that attaches an upstream's static identity — its configured labels
//! and capabilities — to whatever transport/cache/methods stack sits beneath
//! it. Mirrors the metadata the legacy `DefaultUpstream` carries (labels +
//! capabilities + node details).
//!
//! It's installed as the outermost layer during wiring so the `Multistream`
//! holds it directly: `Describe` (and, later, capability/label-based selection)
//! read this metadata through the `RpcUpstream` trait, while every call still
//! flows straight through to the inner upstream.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{Capability, RpcUpstream, UpstreamError};
use std::collections::HashMap;
use std::sync::Arc;

/// Decorates an upstream with its configured `labels` and `capabilities`,
/// delegating all request handling to the inner upstream unchanged.
pub struct IdentifiedUpstream {
    inner: Arc<dyn RpcUpstream>,
    labels: HashMap<String, String>,
    capabilities: Vec<Capability>,
}

impl IdentifiedUpstream {
    /// Wrap `inner`, advertising the given labels and capabilities. The
    /// capability list is deduplicated and always includes [`Capability::Rpc`]
    /// since any upstream that can take a call serves RPC.
    pub fn new(
        inner: Arc<dyn RpcUpstream>,
        labels: HashMap<String, String>,
        capabilities: Vec<Capability>,
    ) -> Self {
        let mut caps = vec![Capability::Rpc];
        for c in capabilities {
            if !caps.contains(&c) {
                caps.push(c);
            }
        }
        Self {
            inner,
            labels,
            capabilities: caps,
        }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for IdentifiedUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        self.inner.call(request).await
    }

    fn id(&self) -> &str {
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

    fn labels(&self) -> &HashMap<String, String> {
        &self.labels
    }

    fn capabilities(&self) -> Vec<Capability> {
        self.capabilities.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::head::NoHead;

    static MOCK_STATE: std::sync::LazyLock<Arc<UpstreamState>> =
        std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));

    struct StubUpstream;

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            Err(UpstreamError::Transport("stub".into()))
        }
        fn id(&self) -> &str {
            "stub"
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::Ok
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            None
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &MOCK_STATE
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn exposes_labels_and_capabilities() {
        let up = IdentifiedUpstream::new(
            Arc::new(StubUpstream),
            labels(&[("provider", "infura"), ("region", "us")]),
            vec![Capability::Balance],
        );
        assert_eq!(up.labels().get("provider").map(String::as_str), Some("infura"));
        // RPC is always present, Balance was requested.
        assert!(up.capabilities().contains(&Capability::Rpc));
        assert!(up.capabilities().contains(&Capability::Balance));
    }

    #[test]
    fn rpc_is_always_present_and_deduplicated() {
        let up = IdentifiedUpstream::new(
            Arc::new(StubUpstream),
            HashMap::new(),
            vec![Capability::Rpc, Capability::Rpc, Capability::Balance],
        );
        let caps = up.capabilities();
        assert_eq!(caps.iter().filter(|c| **c == Capability::Rpc).count(), 1);
        assert_eq!(caps.len(), 2);
    }

    #[test]
    fn delegates_identity_to_inner() {
        let up = IdentifiedUpstream::new(Arc::new(StubUpstream), HashMap::new(), vec![]);
        assert_eq!(up.id(), "stub");
        assert_eq!(up.availability(), UpstreamAvailability::Ok);
    }
}
