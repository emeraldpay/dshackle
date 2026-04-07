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

//! A failover wrapper that tries a primary upstream first and falls back to a
//! secondary if the primary fails or returns an empty result.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// Wraps two upstreams — always calls the primary first, and if it fails,
/// retries the same request on the secondary.
pub struct SwitchClient {
    primary: Arc<dyn RpcUpstream>,
    secondary: Arc<dyn RpcUpstream>,
}

impl SwitchClient {
    pub fn new(primary: Arc<dyn RpcUpstream>, secondary: Arc<dyn RpcUpstream>) -> Self {
        Self { primary, secondary }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for SwitchClient {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        match self.primary.call(request).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                tracing::trace!(error = %e, "primary upstream failed, falling back to secondary");
                self.secondary.call(request).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::JsonRpcResponse;
    use serde_json::value::RawValue;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A mock upstream that always succeeds with a fixed result.
    struct SuccessUpstream {
        calls: AtomicU32,
        value: String,
    }

    impl SuccessUpstream {
        fn new(value: &str) -> Self {
            Self {
                calls: AtomicU32::new(0),
                value: value.to_string(),
            }
        }

        fn call_count(&self) -> u32 {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for SuccessUpstream {
        async fn call(&self, _request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let raw = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#, self.value);
            Ok(serde_json::from_str(&raw).unwrap())
        }
    }

    /// A mock upstream that always fails.
    struct FailUpstream {
        calls: AtomicU32,
    }

    impl FailUpstream {
        fn new() -> Self {
            Self {
                calls: AtomicU32::new(0),
            }
        }

        fn call_count(&self) -> u32 {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for FailUpstream {
        async fn call(&self, _request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(UpstreamError::Transport("mock failure".into()))
        }
    }

    fn dummy_request() -> JsonRpcRequest {
        JsonRpcRequest::new(1, "eth_blockNumber".into(), serde_json::json!([]))
    }

    #[tokio::test]
    async fn uses_primary_when_it_succeeds() {
        let primary = Arc::new(SuccessUpstream::new(r#""0x1""#));
        let secondary = Arc::new(SuccessUpstream::new(r#""0x2""#));
        let client = SwitchClient::new(primary.clone(), secondary.clone());

        let resp = client.call(&dummy_request()).await.unwrap();
        assert_eq!(resp.result.unwrap().get(), r#""0x1""#);
        assert_eq!(primary.call_count(), 1);
        assert_eq!(secondary.call_count(), 0);
    }

    #[tokio::test]
    async fn falls_back_to_secondary_on_primary_failure() {
        let primary = Arc::new(FailUpstream::new());
        let secondary = Arc::new(SuccessUpstream::new(r#""0x2""#));
        let client = SwitchClient::new(primary.clone(), secondary.clone());

        let resp = client.call(&dummy_request()).await.unwrap();
        assert_eq!(resp.result.unwrap().get(), r#""0x2""#);
        assert_eq!(primary.call_count(), 1);
        assert_eq!(secondary.call_count(), 1);
    }

    #[tokio::test]
    async fn returns_secondary_error_when_both_fail() {
        let primary = Arc::new(FailUpstream::new());
        let secondary = Arc::new(FailUpstream::new());
        let client = SwitchClient::new(primary.clone(), secondary.clone());

        let err = client.call(&dummy_request()).await.unwrap_err();
        assert!(matches!(err, UpstreamError::Transport(_)));
        assert_eq!(primary.call_count(), 1);
        assert_eq!(secondary.call_count(), 1);
    }
}
