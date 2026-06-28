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

//! Turns a request body into a JSON-RPC response body: frames it, validates
//! each item, runs it through the shared call path, and assembles the reply.
//! Ports the legacy `HttpHandler` request handling.

use super::protocol::{
    self, BodyKind, CODE_CALL_FAILURE, CODE_INVALID_JSON, ProtocolError, ProxyRequest, RequestId,
};
use crate::jsonrpc::{JsonRpcRequest, RpcMethod};
use crate::rpc::native_call::execute_call;
use crate::upstream::Multistream;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::value::RawValue;

/// Produce the JSON-RPC response body for a request body. The returned string
/// is always valid JSON and is served with HTTP 200 — JSON-RPC errors live in
/// the body, never the HTTP status (matching the legacy proxy).
pub async fn process(body: &[u8], multistream: &Multistream, preserve_order: bool) -> String {
    let kind = match protocol::detect_kind(body) {
        Ok(kind) => kind,
        Err(e) => return protocol::build_protocol_error(&e),
    };
    let parsed = match protocol::parse_body(body) {
        Ok(value) => value,
        Err(e) => return protocol::build_protocol_error(&e),
    };

    match kind {
        BodyKind::Single => match protocol::validate_item(&parsed) {
            Ok(req) => run(multistream, &req).await,
            Err(e) => protocol::build_protocol_error(&e),
        },
        BodyKind::Batch => process_batch(parsed, multistream, preserve_order).await,
    }
}

async fn process_batch(
    parsed: serde_json::Value,
    multistream: &Multistream,
    preserve_order: bool,
) -> String {
    let Some(items) = parsed.as_array() else {
        // First byte was `[` but the value isn't an array — malformed.
        let err = ProtocolError {
            code: CODE_INVALID_JSON,
            message: "Failed to parse JSON".to_string(),
            id: RequestId::fallback(),
        };
        return protocol::build_protocol_error(&err);
    };
    if items.is_empty() {
        return "[]".to_string();
    }

    // Validation is all-or-nothing: a single invalid item aborts the whole
    // batch with one error object, as the legacy proxy does.
    let mut requests = Vec::with_capacity(items.len());
    for item in items {
        match protocol::validate_item(item) {
            Ok(req) => requests.push(req),
            Err(e) => return protocol::build_protocol_error(&e),
        }
    }

    let bodies = if preserve_order {
        // `join_all` resolves concurrently but yields results in request order.
        join_all(requests.iter().map(|req| run(multistream, req))).await
    } else {
        // Emit in completion order — cheaper and the default, since clients
        // correlate by id anyway.
        let mut pending: FuturesUnordered<_> =
            requests.iter().map(|req| run(multistream, req)).collect();
        let mut out = Vec::with_capacity(requests.len());
        while let Some(body) = pending.next().await {
            out.push(body);
        }
        out
    };

    format!("[{}]", bodies.join(","))
}

/// Execute one validated request and serialize its response object. Shared with
/// the WebSocket transport, which dispatches one validated call at a time.
pub(super) async fn run(multistream: &Multistream, req: &ProxyRequest) -> String {
    let method = match req.method.parse::<RpcMethod>() {
        Ok(method) => method,
        Err(_) => {
            // The transport never sees a malformed method name; report it the
            // way a node would rather than forwarding garbage.
            return protocol::build_error(&req.id, -32601, "Method not found".to_string(), None);
        }
    };

    // The upstream response id is ignored, so a fixed internal id is fine; the
    // caller's id is restored when the response is built.
    let request = JsonRpcRequest::new(0, method, req.params.clone());

    match execute_call(multistream, &request).await {
        Ok(resp) => {
            if let Some(err) = resp.error {
                protocol::build_error(&req.id, err.code, err.message, err.data)
            } else if let Some(result) = resp.result {
                protocol::build_success(&req.id, &result)
            } else {
                let null = RawValue::from_string("null".to_string()).expect("valid json");
                protocol::build_success(&req.id, &null)
            }
        }
        Err(e) => protocol::build_error(&req.id, CODE_CALL_FAILURE, e.to_string(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::JsonRpcResponse;
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::quorum::{AlwaysQuorum, CallQuorum, QuorumFactory};
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::{RpcUpstream, UpstreamError};
    use std::sync::Arc;

    /// Upstream that replies with a canned raw result for every call.
    struct StubUpstream {
        result: &'static str,
        state: Arc<UpstreamState>,
    }

    impl StubUpstream {
        fn with_result(result: &'static str) -> Arc<dyn RpcUpstream> {
            Arc::new(Self {
                result,
                state: Arc::new(UpstreamState::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let raw = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#, self.result);
            Ok(serde_json::from_str(&raw).unwrap())
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
            &self.state
        }
    }

    struct AlwaysFactory;
    impl QuorumFactory for AlwaysFactory {
        fn quorum_for(&self, _method: &RpcMethod) -> Box<dyn CallQuorum> {
            Box::new(AlwaysQuorum::new())
        }
    }

    fn multistream(result: &'static str) -> Multistream {
        Multistream::new(
            vec![StubUpstream::with_result(result)],
            Arc::new(AlwaysFactory),
        )
    }

    #[tokio::test]
    async fn single_request_echoes_id_and_result() {
        let ms = multistream(r#""0xabc""#);
        let body = process(
            br#"{"jsonrpc":"2.0","id":42,"method":"eth_blockNumber","params":[]}"#,
            &ms,
            false,
        )
        .await;
        assert_eq!(body, r#"{"jsonrpc":"2.0","id":42,"result":"0xabc"}"#);
    }

    #[tokio::test]
    async fn batch_returns_array_in_request_order() {
        let ms = multistream(r#""0x1""#);
        let body = process(
            br#"[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]"#,
            &ms,
            true, // preserve order
        )
        .await;
        assert_eq!(
            body,
            r#"[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x1"}]"#
        );
    }

    #[tokio::test]
    async fn empty_batch_returns_empty_array() {
        let ms = multistream(r#""0x1""#);
        let body = process(b"[]", &ms, false).await;
        assert_eq!(body, "[]");
    }

    #[tokio::test]
    async fn parse_error_returns_single_object_with_fallback_id() {
        let ms = multistream(r#""0x1""#);
        let body = process(b"not json", &ms, false).await;
        assert_eq!(
            body,
            r#"{"jsonrpc":"2.0","id":-1,"error":{"code":-32700,"message":"Failed to parse JSON"}}"#
        );
    }

    #[tokio::test]
    async fn invalid_item_in_batch_aborts_whole_batch() {
        let ms = multistream(r#""0x1""#);
        // Second item is missing its id — the whole batch fails with one error.
        let body = process(
            br#"[{"jsonrpc":"2.0","id":1,"method":"m"},{"jsonrpc":"2.0","method":"m"}]"#,
            &ms,
            false,
        )
        .await;
        assert_eq!(
            body,
            r#"{"jsonrpc":"2.0","id":-1,"error":{"code":-32600,"message":"ID is not set"}}"#
        );
    }

    #[tokio::test]
    async fn upstream_error_is_forwarded() {
        // Upstream returns a JSON-RPC error object; it should pass through verbatim.
        struct ErrUpstream(Arc<UpstreamState>);
        #[async_trait::async_trait]
        impl RpcUpstream for ErrUpstream {
            async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
                Ok(serde_json::from_str(
                    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#,
                )
                .unwrap())
            }
            fn id(&self) -> &str {
                "err"
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
                &self.0
            }
        }

        let ms = Multistream::new(
            vec![Arc::new(ErrUpstream(Arc::new(UpstreamState::new()))) as Arc<dyn RpcUpstream>],
            Arc::new(AlwaysFactory),
        );
        let body = process(
            br#"{"jsonrpc":"2.0","id":9,"method":"eth_foo"}"#,
            &ms,
            false,
        )
        .await;
        assert_eq!(
            body,
            r#"{"jsonrpc":"2.0","id":9,"error":{"code":-32601,"message":"Method not found"}}"#
        );
    }
}
