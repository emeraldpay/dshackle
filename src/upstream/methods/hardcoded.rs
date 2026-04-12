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

//! Upstream wrapper that intercepts configured methods with static JSON-RPC
//! responses, without hitting the actual upstream node.
//!
//! Used for methods like `net_version`, `eth_chainId`, `web3_clientVersion`
//! whose answers are known at configuration time.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Returns predefined responses for configured methods, delegating
/// everything else to the inner upstream.
pub struct HardcodedMethods {
    delegate: Arc<dyn RpcUpstream>,
    responses: HashMap<RpcMethod, Box<RawValue>>,
}

impl HardcodedMethods {
    pub fn new(
        delegate: Arc<dyn RpcUpstream>,
        responses: HashMap<RpcMethod, Box<RawValue>>,
    ) -> Self {
        Self { delegate, responses }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for HardcodedMethods {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        if let Some(result) = self.responses.get(&request.method) {
            tracing::trace!(upstream = self.delegate.id(), method = %request.method, "hardcoded response");
            return Ok(JsonRpcResponse {
                id: serde_json::Value::from(request.id),
                result: Some(result.clone()),
                error: None,
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
        async fn call(&self, _request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let raw = r#"{"jsonrpc":"2.0","id":1,"result":"0xfromupstream"}"#;
            Ok(serde_json::from_str(raw).unwrap())
        }
        fn id(&self) -> &str { "stub" }
        fn availability(&self) -> UpstreamAvailability { UpstreamAvailability::Ok }
        fn head(&self) -> &dyn Head { &NoHead }
        fn lag(&self) -> Option<u64> { None }
        fn state(&self) -> &Arc<UpstreamState> { &MOCK_STATE }
    }

    fn request(method: &str) -> JsonRpcRequest {
        JsonRpcRequest::new(1, method.into(), serde_json::json!([]))
    }

    #[tokio::test]
    async fn returns_hardcoded_for_known_method() {
        let mut responses = HashMap::new();
        responses.insert(
            "net_version".into(),
            RawValue::from_string("\"1\"".into()).unwrap(),
        );
        let hc = HardcodedMethods::new(Arc::new(StubUpstream), responses);

        let resp = hc.call(&request("net_version")).await.unwrap();
        assert_eq!(resp.result.unwrap().get(), "\"1\"");
        // id should match the request
        assert_eq!(resp.id, serde_json::Value::from(1u32));
    }

    #[tokio::test]
    async fn delegates_unknown_method_to_upstream() {
        let responses = HashMap::new();
        let hc = HardcodedMethods::new(Arc::new(StubUpstream), responses);

        let resp = hc.call(&request("eth_blockNumber")).await.unwrap();
        assert_eq!(resp.result.unwrap().get(), r#""0xfromupstream""#);
    }

    #[tokio::test]
    async fn preserves_request_id_in_hardcoded_response() {
        let mut responses = HashMap::new();
        responses.insert(
            "eth_chainId".into(),
            RawValue::from_string("\"0x1\"".into()).unwrap(),
        );
        let hc = HardcodedMethods::new(Arc::new(StubUpstream), responses);

        let req = JsonRpcRequest::new(42, "eth_chainId".into(), serde_json::json!([]));
        let resp = hc.call(&req).await.unwrap();
        assert_eq!(resp.id, serde_json::Value::from(42u32));
    }

    #[tokio::test]
    async fn delegates_identity() {
        let hc = HardcodedMethods::new(Arc::new(StubUpstream), HashMap::new());

        assert_eq!(hc.id(), "stub");
        assert_eq!(hc.availability(), UpstreamAvailability::Ok);
    }
}
