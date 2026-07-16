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
use crate::blockchain::TargetBlockchain;
use crate::jsonrpc::{JsonRpcRequest, RpcMethod};
use crate::logs;
use crate::logs::access;
use crate::metrics;
use crate::rpc::native_call::execute_call;
use crate::upstream::Multistream;
use crate::upstream::selector::LabelSelector;
use crate::upstream::traits::UpstreamError;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::value::RawValue;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Access-log identity shared by every call of one proxy request: the client
/// request details plus the batch position counters. `None` when the access
/// log is off.
#[derive(Clone)]
pub(super) struct AccessMeta {
    pub channel: logs::Channel,
    pub ctx: Arc<logs::IngressContext>,
    /// Calls in the batch (1 for a single request).
    pub total: usize,
    /// Reply order across the batch — the legacy `index` increments as
    /// replies complete, not in request order.
    pub index: Arc<AtomicUsize>,
}

impl AccessMeta {
    pub fn new(channel: logs::Channel, ctx: Arc<logs::IngressContext>) -> Option<Self> {
        logs::access_enabled().then(|| Self {
            channel,
            ctx,
            total: 1,
            index: Arc::new(AtomicUsize::new(0)),
        })
    }
}

/// Produce the JSON-RPC response body for a request body. The returned string
/// is always valid JSON and is served with HTTP 200 — JSON-RPC errors live in
/// the body, never the HTTP status (matching the legacy proxy).
pub async fn process(
    body: &[u8],
    multistream: &Multistream,
    chain: &TargetBlockchain,
    preserve_order: bool,
    access: Option<AccessMeta>,
) -> String {
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
            Ok(req) => run(multistream, chain, &req, access.as_ref(), 0).await,
            Err(e) => protocol::build_protocol_error(&e),
        },
        BodyKind::Batch => process_batch(parsed, multistream, chain, preserve_order, access).await,
    }
}

async fn process_batch(
    parsed: serde_json::Value,
    multistream: &Multistream,
    chain: &TargetBlockchain,
    preserve_order: bool,
    access: Option<AccessMeta>,
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

    // Every item of the batch shares the meta; only `total` reflects the
    // batch size now that it is known.
    let access = access.map(|meta| AccessMeta {
        total: requests.len(),
        ..meta
    });

    let bodies = if preserve_order {
        // `join_all` resolves concurrently but yields results in request order.
        join_all(
            requests
                .iter()
                .enumerate()
                .map(|(pos, req)| run(multistream, chain, req, access.as_ref(), pos)),
        )
        .await
    } else {
        // Emit in completion order — cheaper and the default, since clients
        // correlate by id anyway.
        let mut pending: FuturesUnordered<_> = requests
            .iter()
            .enumerate()
            .map(|(pos, req)| run(multistream, chain, req, access.as_ref(), pos))
            .collect();
        let mut out = Vec::with_capacity(requests.len());
        while let Some(body) = pending.next().await {
            out.push(body);
        }
        out
    };

    format!("[{}]", bodies.join(","))
}

/// What a call ended with, for the access log record.
struct CallOutcome {
    succeed: bool,
    /// Response and error payloads, filled only under `include-messages`.
    response: Option<String>,
    error: Option<String>,
}

/// Execute one validated request and serialize its response object. Shared with
/// the WebSocket transport, which dispatches one validated call at a time.
/// `pos` is the call's position in its batch (0 for a single request).
pub(super) async fn run(
    multistream: &Multistream,
    chain: &TargetBlockchain,
    req: &ProxyRequest,
    access: Option<&AccessMeta>,
    pos: usize,
) -> String {
    metrics::jsonrpc_request(chain, &req.method);
    let start = std::time::Instant::now();
    let (body, outcome) = execute(multistream, chain, req).await;
    metrics::jsonrpc_call(chain, &req.method, start.elapsed());

    if let Some(meta) = access {
        // The params are the "payload" of a proxy call, matching the legacy
        // proxy that carried the params JSON as the item payload.
        let params_size = params_json_size(&req.params) as u64;
        logs::access_log(&access::AccessRecord::new(
            meta.channel,
            access::Payload::NativeCall(access::OnBlockchain::new(
                Some(chain),
                access::NativeCall {
                    request: meta.ctx.request_details(),
                    total: meta.total,
                    index: meta.index.fetch_add(1, Ordering::Relaxed),
                    selector: None,
                    quorum: None,
                    min_availability: None,
                    succeed: outcome.succeed,
                    rpc_error: None,
                    payload_size_bytes: params_size,
                    native_call: access::NativeCallItemDetails {
                        method: req.method.clone(),
                        id: pos as u32,
                        payload_size_bytes: params_size,
                        nonce: 0,
                        request_params: logs::include_messages().then(|| req.params.to_string()),
                    },
                    response_body: outcome.response,
                    error_message: outcome.error,
                    nonce: None,
                    signature: None,
                },
            )),
        ));
    }

    body
}

/// The byte size of the params payload — zero when the request carried none
/// (the legacy proxy treated absent params as an empty payload).
fn params_json_size(params: &serde_json::Value) -> usize {
    if params.is_null() {
        0
    } else {
        params.to_string().len()
    }
}

async fn execute(
    multistream: &Multistream,
    chain: &TargetBlockchain,
    req: &ProxyRequest,
) -> (String, CallOutcome) {
    let include = logs::access_enabled() && logs::include_messages();
    let method = match req.method.parse::<RpcMethod>() {
        Ok(method) => method,
        Err(_) => {
            // The transport never sees a malformed method name; report it the
            // way a node would rather than forwarding garbage.
            metrics::jsonrpc_err(chain, &req.method);
            let body = protocol::build_error(&req.id, -32601, "Method not found".to_string(), None);
            return (
                body,
                CallOutcome {
                    succeed: false,
                    response: None,
                    error: include.then(|| "Method not found".to_string()),
                },
            );
        }
    };

    // The upstream response id is ignored, so a fixed internal id is fine; the
    // caller's id is restored when the response is built.
    let request = JsonRpcRequest::new(0, method, req.params.clone());

    // Proxy requests carry no label selector — that's a gRPC-only input.
    match execute_call(multistream, &request, &LabelSelector::Any).await {
        Ok(routed) => {
            let resp = routed.response;
            if let Some(err) = resp.error {
                metrics::jsonrpc_err(chain, &req.method);
                let outcome = CallOutcome {
                    succeed: false,
                    response: None,
                    error: include.then(|| err.message.clone()),
                };
                (
                    protocol::build_error(&req.id, err.code, err.message, err.data),
                    outcome,
                )
            } else if let Some(result) = resp.result {
                let outcome = CallOutcome {
                    succeed: true,
                    response: include.then(|| result.get().to_string()),
                    error: None,
                };
                (protocol::build_success(&req.id, &result), outcome)
            } else {
                let null = RawValue::from_string("null".to_string()).expect("valid json");
                let outcome = CallOutcome {
                    succeed: true,
                    response: include.then(|| "null".to_string()),
                    error: None,
                };
                (protocol::build_success(&req.id, &null), outcome)
            }
        }
        Err(e) => {
            metrics::jsonrpc_fail(chain, &req.method);
            // An unsupported method keeps its distinct legacy code; every
            // other failure is a generic routing/quorum error.
            let code = match &e {
                UpstreamError::MethodNotAllowed(_) => protocol::CODE_METHOD_NOT_EXIST,
                _ => CODE_CALL_FAILURE,
            };
            let outcome = CallOutcome {
                succeed: false,
                response: None,
                error: include.then(|| e.to_string()),
            };
            (
                protocol::build_error(&req.id, code, e.to_string(), None),
                outcome,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::JsonRpcResponse;
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::id::UpstreamId;
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
            &self.state
        }
    }

    struct AlwaysFactory;
    impl QuorumFactory for AlwaysFactory {
        fn quorum_for(&self, _method: &RpcMethod) -> Box<dyn CallQuorum> {
            Box::new(AlwaysQuorum::new())
        }
    }

    fn chain() -> TargetBlockchain {
        "ethereum".parse().unwrap()
    }

    fn multistream(result: &'static str) -> Multistream {
        Multistream::new(
            TargetBlockchain::Standard(emerald_api::proto::common::ChainRef::ChainEthereum),
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
            &chain(),
            false,
            None,
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
            &chain(),
            true, // preserve order,
            None
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
        let body = process(b"[]", &ms, &chain(), false, None).await;
        assert_eq!(body, "[]");
    }

    #[tokio::test]
    async fn parse_error_returns_single_object_with_fallback_id() {
        let ms = multistream(r#""0x1""#);
        let body = process(b"not json", &ms, &chain(), false, None).await;
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
            &chain(),
            false,
            None,
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
            fn id(&self) -> &UpstreamId {
                static ID: std::sync::LazyLock<UpstreamId> =
                    std::sync::LazyLock::new(|| "err".parse().unwrap());
                &ID
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
            TargetBlockchain::Standard(emerald_api::proto::common::ChainRef::ChainEthereum),
            vec![Arc::new(ErrUpstream(Arc::new(UpstreamState::new()))) as Arc<dyn RpcUpstream>],
            Arc::new(AlwaysFactory),
        );
        let body = process(
            br#"{"jsonrpc":"2.0","id":9,"method":"eth_foo"}"#,
            &ms,
            &chain(),
            false,
            None,
        )
        .await;
        assert_eq!(
            body,
            r#"{"jsonrpc":"2.0","id":9,"error":{"code":-32601,"message":"Method not found"}}"#
        );
    }
}
