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

//! Wrapper that attaches an upstream's static identity — its label sets,
//! capabilities, and routing role — to whatever transport/cache/methods stack
//! sits beneath it. Mirrors the metadata the legacy `DefaultUpstream` carries
//! (labels + capabilities + node details).
//!
//! It's installed as the outermost layer during wiring so the `Multistream`
//! holds it directly: `Describe` and capability/label-based selection read
//! this metadata through the `RpcUpstream` trait, while every call still
//! flows straight through to the inner upstream.

use crate::config::upstreams::UpstreamRole;
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::id::UpstreamId;
use crate::upstream::label::UpstreamLabels;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{Capability, RpcUpstream, UpstreamError};
use std::sync::Arc;

/// Decorates an upstream with its label sets, `capabilities`, and routing
/// `role`, delegating all request handling to the inner upstream unchanged.
pub struct IdentifiedUpstream {
    inner: Arc<dyn RpcUpstream>,
    label_sets: Vec<UpstreamLabels>,
    capabilities: Vec<Capability>,
    role: UpstreamRole,
}

impl IdentifiedUpstream {
    /// Wrap `inner`, advertising the given label sets, capabilities
    /// (deduplicated, order preserved), and routing role.
    ///
    /// A local upstream passes a single set — its configured labels — while a
    /// Dshackle relay passes one set per node the remote advertised via
    /// `Describe` (legacy `GrpcUpstreamStatus`), so label selectors keep
    /// working through a relay without duplicating the remote's labels in the
    /// local config.
    ///
    /// Capabilities are advertised exactly as given — not upgraded. Local
    /// upstreams pass [`Capability::Rpc`] explicitly (via `local_capabilities`),
    /// while a remote Dshackle passes only what it reported over `Describe`. A
    /// balance-only relay (`Balance` without `Rpc`) must stay balance-only, or it
    /// would wrongly be picked for call routing (legacy `RemoteCapabilities`).
    pub fn new(
        inner: Arc<dyn RpcUpstream>,
        label_sets: Vec<UpstreamLabels>,
        capabilities: Vec<Capability>,
        role: UpstreamRole,
    ) -> Self {
        let mut caps: Vec<Capability> = Vec::new();
        for c in capabilities {
            if !caps.contains(&c) {
                caps.push(c);
            }
        }
        Self {
            inner,
            label_sets,
            capabilities: caps,
            role,
        }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for IdentifiedUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        self.inner.call(request).await
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
        &self.label_sets
    }

    fn capabilities(&self) -> Vec<Capability> {
        self.capabilities.clone()
    }

    fn role(&self) -> UpstreamRole {
        self.role
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
        fn id(&self) -> &UpstreamId {
            crate::upstream::id::stub_id()
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

    use crate::upstream::label::test_labels as labels;

    #[test]
    fn exposes_labels_and_capabilities() {
        let up = IdentifiedUpstream::new(
            Arc::new(StubUpstream),
            vec![labels(&[("provider", "infura"), ("region", "us")])],
            vec![Capability::Rpc, Capability::Balance],
            UpstreamRole::Primary,
        );
        assert_eq!(up.label_sets()[0].get("provider"), Some("infura"));
        assert!(up.capabilities().contains(&Capability::Rpc));
        assert!(up.capabilities().contains(&Capability::Balance));
    }

    #[test]
    fn carries_multiple_label_sets() {
        // The shape of a Dshackle relay: one set per node the remote advertised.
        let up = IdentifiedUpstream::new(
            Arc::new(StubUpstream),
            vec![
                labels(&[("archive", "true")]),
                labels(&[("archive", "false")]),
            ],
            vec![Capability::Rpc],
            UpstreamRole::Primary,
        );
        assert_eq!(up.label_sets().len(), 2);
    }

    #[test]
    fn deduplicates_capabilities() {
        let up = IdentifiedUpstream::new(
            Arc::new(StubUpstream),
            vec![],
            vec![Capability::Rpc, Capability::Rpc, Capability::Balance],
            UpstreamRole::Primary,
        );
        let caps = up.capabilities();
        assert_eq!(caps.iter().filter(|c| **c == Capability::Rpc).count(), 1);
        assert_eq!(caps.len(), 2);
    }

    #[test]
    fn balance_only_is_not_upgraded_to_rpc() {
        // A remote Dshackle that reported only BALANCE must not gain RPC, or it
        // would wrongly be selected for call routing.
        let up = IdentifiedUpstream::new(
            Arc::new(StubUpstream),
            vec![],
            vec![Capability::Balance],
            UpstreamRole::Primary,
        );
        assert!(!up.capabilities().contains(&Capability::Rpc));
        assert!(up.capabilities().contains(&Capability::Balance));
    }

    #[test]
    fn delegates_identity_to_inner() {
        let up = IdentifiedUpstream::new(
            Arc::new(StubUpstream),
            vec![],
            vec![],
            UpstreamRole::Primary,
        );
        assert_eq!(up.id(), "stub");
        assert_eq!(up.availability(), UpstreamAvailability::Ok);
    }
}
