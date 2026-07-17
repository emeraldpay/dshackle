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

//! Per-transport routing preference (the WS `disabled-methods` config option).
//!
//! Unlike the upstream-level methods config, which declares what the *node*
//! can serve, this steers specific methods away from one *transport* of the
//! upstream: a method with very large responses is better consumed over HTTP
//! than through a WS frame, so it is disabled on the WS leg and served by the
//! HTTP fallback instead. Legacy did this by returning an empty result from
//! `WsConnectionImpl.callRpc`, which the WS/HTTP switch turned into an HTTP
//! retry; here the divert is an explicit error, which the
//! [`SwitchClient`](crate::upstream::switch::SwitchClient) handles the same
//! way.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::id::UpstreamId;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::collections::HashSet;
use std::sync::Arc;

/// Answers the listed methods with [`UpstreamError::MethodNotAllowed`] instead
/// of forwarding them to the wrapped transport, which sends the switch to the
/// other leg; everything else passes through.
pub struct DisabledMethods {
    delegate: Arc<dyn RpcUpstream>,
    disabled: HashSet<RpcMethod>,
}

impl DisabledMethods {
    pub fn new(
        delegate: Arc<dyn RpcUpstream>,
        disabled: impl IntoIterator<Item = RpcMethod>,
    ) -> Self {
        Self {
            delegate,
            disabled: disabled.into_iter().collect(),
        }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for DisabledMethods {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        if self.disabled.contains(&request.method) {
            return Err(UpstreamError::MethodNotAllowed(request.method.to_string()));
        }
        self.delegate.call(request).await
    }

    fn id(&self) -> &UpstreamId {
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
        !self.disabled.contains(method) && self.delegate.allows_method(method)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::head::NoHead;

    static MOCK_STATE: std::sync::LazyLock<Arc<UpstreamState>> =
        std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));

    #[derive(Default)]
    struct StubUpstream {
        calls: std::sync::atomic::AtomicU32,
    }

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let raw = r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#;
            Ok(serde_json::from_str(raw).unwrap())
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

    fn disabled(delegate: Arc<StubUpstream>, methods: &[&str]) -> DisabledMethods {
        DisabledMethods::new(delegate, methods.iter().map(|m| RpcMethod::from(*m)))
    }

    fn request(method: &str) -> JsonRpcRequest {
        JsonRpcRequest::new(1, method.into(), serde_json::json!([]))
    }

    #[tokio::test]
    async fn disabled_method_diverted_without_reaching_transport() {
        let transport = Arc::new(StubUpstream::default());
        let filter = disabled(Arc::clone(&transport), &["debug_traceBlockByNumber"]);
        let err = filter
            .call(&request("debug_traceBlockByNumber"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, UpstreamError::MethodNotAllowed(m) if m == "debug_traceBlockByNumber")
        );
        assert!(!filter.allows_method(&"debug_traceBlockByNumber".into()));
        assert_eq!(
            transport.calls.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn unlisted_method_passes_through() {
        let transport = Arc::new(StubUpstream::default());
        let filter = disabled(transport, &["debug_traceBlockByNumber"]);
        assert!(filter.call(&request("eth_blockNumber")).await.is_ok());
        assert!(filter.allows_method(&"eth_blockNumber".into()));
    }

    /// The wiring this exists for: on a WS-primary/HTTP-fallback upstream a
    /// WS-disabled method must still succeed — served by the HTTP leg.
    #[tokio::test]
    async fn switch_serves_disabled_method_via_fallback() {
        let ws = Arc::new(StubUpstream::default());
        let http = Arc::new(StubUpstream::default());
        let switch = crate::upstream::switch::SwitchClient::new(
            Arc::new(disabled(Arc::clone(&ws), &["debug_traceBlockByNumber"])),
            Arc::clone(&http) as Arc<dyn RpcUpstream>,
        );

        let resp = switch.call(&request("debug_traceBlockByNumber")).await;
        assert!(resp.is_ok());
        assert_eq!(ws.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(http.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
