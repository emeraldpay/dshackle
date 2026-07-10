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

//! Upstream wrapper that answers configured methods with a static JSON-RPC
//! response taken from a [`QuorumFactory`] methods config, without hitting
//! the actual upstream node.
//!
//! Used for methods like `net_version`, `eth_chainId`, `web3_clientVersion`
//! whose answers are known at configuration time.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::quorum::QuorumFactory;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// Returns predefined responses for methods the backing [`QuorumFactory`]
/// knows as hardcoded, delegating everything else to the inner upstream.
pub struct HardcodedMethods {
    delegate: Arc<dyn RpcUpstream>,
    methods: Arc<dyn QuorumFactory>,
}

impl HardcodedMethods {
    pub fn new(delegate: Arc<dyn RpcUpstream>, methods: Arc<dyn QuorumFactory>) -> Self {
        Self { delegate, methods }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for HardcodedMethods {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        if let Some(result) = self.methods.hardcoded_response(&request.method) {
            tracing::trace!(upstream = self.delegate.id(), method = %request.method, "hardcoded response");
            return Ok(JsonRpcResponse {
                id: serde_json::Value::from(request.id),
                result: Some(result.to_owned()),
                error: None,
                provided_signature: None,
            });
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
        // A hardcoded response is always answerable here; otherwise fall
        // through to whatever the delegate's own allow-list says.
        self.methods.is_hardcoded(method) || self.delegate.allows_method(method)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::{MethodConfig, MethodsConfig};
    use crate::upstream::head::NoHead;
    use crate::upstream::methods::DefaultMethods;
    use crate::upstream::methods::config::ConfiguredMethods;

    static MOCK_STATE: std::sync::LazyLock<Arc<UpstreamState>> =
        std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));

    struct StubUpstream;

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let raw = r#"{"jsonrpc":"2.0","id":1,"result":"0xfromupstream"}"#;
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

    fn with_static(method: &str, value: &str) -> Arc<ConfiguredMethods> {
        Arc::new(ConfiguredMethods::from_config(Some(&MethodsConfig {
            enabled: vec![MethodConfig {
                name: method.into(),
                quorum: None,
                static_value: Some(value.into()),
            }],
            disabled: vec![],
        })))
    }

    #[tokio::test]
    async fn returns_hardcoded_for_known_method() {
        let methods = with_static("net_version", "\"1\"");
        let hc = HardcodedMethods::new(Arc::new(StubUpstream), methods as Arc<dyn QuorumFactory>);

        let resp = hc.call(&request("net_version")).await.unwrap();
        assert_eq!(resp.result.unwrap().get(), "\"1\"");
        assert_eq!(resp.id, serde_json::Value::from(1u32));
    }

    #[tokio::test]
    async fn delegates_unknown_method_to_upstream() {
        let hc = HardcodedMethods::new(
            Arc::new(StubUpstream),
            Arc::new(DefaultMethods) as Arc<dyn QuorumFactory>,
        );

        let resp = hc.call(&request("eth_blockNumber")).await.unwrap();
        assert_eq!(resp.result.unwrap().get(), r#""0xfromupstream""#);
    }

    #[tokio::test]
    async fn preserves_request_id_in_hardcoded_response() {
        let methods = with_static("eth_chainId", "\"0x1\"");
        let hc = HardcodedMethods::new(Arc::new(StubUpstream), methods as Arc<dyn QuorumFactory>);

        let req = JsonRpcRequest::new(42, "eth_chainId".into(), serde_json::json!([]));
        let resp = hc.call(&req).await.unwrap();
        assert_eq!(resp.id, serde_json::Value::from(42u32));
    }

    #[tokio::test]
    async fn delegates_identity() {
        let hc = HardcodedMethods::new(
            Arc::new(StubUpstream),
            Arc::new(DefaultMethods) as Arc<dyn QuorumFactory>,
        );

        assert_eq!(hc.id(), "stub");
        assert_eq!(hc.availability(), UpstreamAvailability::Ok);
    }
}
