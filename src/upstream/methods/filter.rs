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

//! Upstream wrapper that rejects calls to methods not recognised by a
//! [`QuorumFactory`] (callable or hardcoded).
//!
//! Sits between the hardcoded-response layer and the actual upstream transport,
//! ensuring only known/supported methods reach the node.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::quorum::QuorumFactory;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// Filters RPC calls, rejecting methods the underlying methods config does
/// not recognise with [`UpstreamError::MethodNotAllowed`].
pub struct MethodFilter {
    delegate: Arc<dyn RpcUpstream>,
    methods: Arc<dyn QuorumFactory>,
}

impl MethodFilter {
    pub fn new(delegate: Arc<dyn RpcUpstream>, methods: Arc<dyn QuorumFactory>) -> Self {
        Self { delegate, methods }
    }

    fn supported(&self, method: &RpcMethod) -> bool {
        self.methods.is_callable(method) || self.methods.is_hardcoded(method)
    }
}

#[async_trait::async_trait]
impl RpcUpstream for MethodFilter {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        if !self.supported(&request.method) {
            return Err(UpstreamError::MethodNotAllowed(request.method.to_string()));
        }
        self.delegate.call(request).await
    }

    fn id(&self) -> &str {
        self.delegate.id()
    }

    fn availability(&self) -> UpstreamAvailability {
        self.delegate.availability()
    }

    fn head(&self) -> &dyn Head {
        self.delegate.head()
    }

    fn lag(&self) -> Option<u64> {
        self.delegate.lag()
    }

    fn state(&self) -> &Arc<UpstreamState> {
        self.delegate.state()
    }

    fn allows_method(&self, method: &RpcMethod) -> bool {
        self.supported(method) && self.delegate.allows_method(method)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::{MethodConfig, MethodsConfig};
    use crate::upstream::head::NoHead;
    use crate::upstream::methods::DefaultMethods;
    use crate::upstream::methods::LayeredMethods;
    use crate::upstream::methods::config::ConfiguredMethods;

    static MOCK_STATE: std::sync::LazyLock<Arc<UpstreamState>> =
        std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));

    struct StubUpstream;

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let raw = r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#;
            Ok(serde_json::from_str(raw).unwrap())
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

    fn request(method: &str) -> JsonRpcRequest {
        JsonRpcRequest::new(1, method.into(), serde_json::json!([]))
    }

    fn layered_enabled(methods: &[&str]) -> Arc<dyn QuorumFactory> {
        let enabled: Vec<MethodConfig> = methods
            .iter()
            .map(|m| MethodConfig {
                name: (*m).into(),
                quorum: None,
                static_value: None,
            })
            .collect();
        let configured = Arc::new(ConfiguredMethods::from_config(Some(&MethodsConfig {
            enabled,
            disabled: vec![],
        })));
        Arc::new(LayeredMethods::new(
            Arc::new(DefaultMethods) as Arc<dyn QuorumFactory>,
            configured,
        ))
    }

    #[tokio::test]
    async fn allows_listed_method() {
        // DefaultMethods considers everything callable by default.
        let filter = MethodFilter::new(
            Arc::new(StubUpstream),
            Arc::new(DefaultMethods) as Arc<dyn QuorumFactory>,
        );
        let resp = filter.call(&request("eth_blockNumber")).await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn rejects_unlisted_method() {
        // ConfiguredMethods alone claims only what the user enabled.
        let methods = Arc::new(ConfiguredMethods::from_config(Some(&MethodsConfig {
            enabled: vec![MethodConfig {
                name: "eth_blockNumber".into(),
                quorum: None,
                static_value: None,
            }],
            disabled: vec![],
        })));
        let filter = MethodFilter::new(Arc::new(StubUpstream), methods as Arc<dyn QuorumFactory>);

        let err = filter
            .call(&request("debug_traceTransaction"))
            .await
            .unwrap_err();
        assert!(matches!(err, UpstreamError::MethodNotAllowed(m) if m == "debug_traceTransaction"));
    }

    #[tokio::test]
    async fn uses_layered_config_union() {
        // Layered methods accept whatever is enabled by either layer.
        let filter = MethodFilter::new(Arc::new(StubUpstream), layered_enabled(&["parity_trace"]));
        // DefaultMethods claims everything callable, so any method passes —
        // a more interesting test is below with ConfiguredMethods alone.
        assert!(filter.call(&request("parity_trace")).await.is_ok());
    }

    #[tokio::test]
    async fn delegates_identity() {
        let filter = MethodFilter::new(
            Arc::new(StubUpstream),
            Arc::new(DefaultMethods) as Arc<dyn QuorumFactory>,
        );

        assert_eq!(filter.id(), "stub");
        assert_eq!(filter.availability(), UpstreamAvailability::Ok);
    }
}
