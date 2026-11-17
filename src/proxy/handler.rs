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
use crate::metrics::{self, DownstreamProtocol, RequestStatus};
use crate::rpc::native_call::{self, execute_call};
use crate::upstream::Multistream;
use crate::upstream::selector::LabelSelector;
use crate::upstream::traits::UpstreamError;
use futures::FutureExt;
use futures::stream::{self, FuturesOrdered, FuturesUnordered, Stream, StreamExt};
use serde_json::value::RawValue;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::task::AbortOnDropHandle;

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

/// The JSON-RPC response to one HTTP request. Either way it's valid JSON and
/// served with HTTP 200: JSON-RPC errors live in the body, never the HTTP
/// status (matching the legacy proxy).
pub(super) enum ResponseBody {
    /// One complete JSON value: a single call's response, or the error that
    /// refused the request as a whole.
    Complete(String),
    /// A batch array in pieces, each sent as soon as it's ready so a fast call
    /// isn't held back by a slow one. Concatenated, they form `[a,b,c]`.
    Chunks(ChunkStream),
}

/// Pieces of a streamed response body. `Sync` because warp requires it of a
/// body stream.
pub(super) type ChunkStream = Pin<Box<dyn Stream<Item = String> + Send + Sync>>;

/// Produce the JSON-RPC response for a request body.
///
/// The upstream calls run in the request's log `ctx`. A batch's calls run as
/// tasks of their own, which don't inherit it from the caller, so it's passed
/// in to be applied to each.
pub async fn process(
    body: &[u8],
    multistream: Arc<Multistream>,
    chain: TargetBlockchain,
    preserve_order: bool,
    ctx: Arc<logs::IngressContext>,
    access: Option<AccessMeta>,
) -> ResponseBody {
    let kind = match protocol::detect_kind(body) {
        Ok(kind) => kind,
        Err(e) => return ResponseBody::Complete(protocol::build_protocol_error(&e)),
    };
    let parsed = match protocol::parse_body(body) {
        Ok(value) => value,
        Err(e) => return ResponseBody::Complete(protocol::build_protocol_error(&e)),
    };

    match kind {
        BodyKind::Single => match protocol::validate_item(&parsed) {
            Ok(req) => {
                let response = logs::with_context(
                    (*ctx).clone(),
                    run(
                        &multistream,
                        DownstreamProtocol::HttpJsonRpc,
                        &chain,
                        &req,
                        access.as_ref(),
                        0,
                    ),
                )
                .await;
                ResponseBody::Complete(response)
            }
            Err(e) => ResponseBody::Complete(protocol::build_protocol_error(&e)),
        },
        BodyKind::Batch => process_batch(parsed, multistream, chain, preserve_order, ctx, access),
    }
}

/// Validates the whole batch up front, since an invalid item turns the
/// response into a single error object, and only then starts streaming.
fn process_batch(
    parsed: serde_json::Value,
    multistream: Arc<Multistream>,
    chain: TargetBlockchain,
    preserve_order: bool,
    ctx: Arc<logs::IngressContext>,
    access: Option<AccessMeta>,
) -> ResponseBody {
    let Some(items) = parsed.as_array() else {
        // First byte was `[` but the value isn't an array — malformed.
        let err = ProtocolError {
            code: CODE_INVALID_JSON,
            message: "Failed to parse JSON".to_string(),
            id: RequestId::fallback(),
        };
        return ResponseBody::Complete(protocol::build_protocol_error(&err));
    };
    if items.is_empty() {
        return ResponseBody::Complete("[]".to_string());
    }

    // Validation is all-or-nothing: a single invalid item aborts the whole
    // batch with one error object, as the legacy proxy does.
    let mut requests = Vec::with_capacity(items.len());
    for item in items {
        match protocol::validate_item(item) {
            Ok(req) => requests.push(req),
            Err(e) => return ResponseBody::Complete(protocol::build_protocol_error(&e)),
        }
    }

    // Every item of the batch shares the meta; only `total` reflects the
    // batch size now that it is known.
    let access = access.map(|meta| AccessMeta {
        total: requests.len(),
        ..meta
    });

    // Each call runs as a task of its own, so it completes even while a slow
    // client holds the response stream back. Dropping the stream (the client
    // disconnects) aborts the calls still in flight.
    let calls = requests.into_iter().enumerate().map(move |(pos, req)| {
        let started = std::time::Instant::now();
        let id = req.id.clone();
        let method = req.method.clone();
        let task = {
            let multistream = Arc::clone(&multistream);
            let access = access.clone();
            AbortOnDropHandle::new(tokio::spawn(logs::with_context(
                (*ctx).clone(),
                async move {
                    run(
                        &multistream,
                        DownstreamProtocol::HttpJsonRpc,
                        &chain,
                        &req,
                        access.as_ref(),
                        pos,
                    )
                    .await
                },
            )))
        };
        let multistream = Arc::clone(&multistream);
        task.map(move |joined| {
            joined.unwrap_or_else(|e| {
                // The task died before it recorded its call.
                metrics::downstream_call(
                    DownstreamProtocol::HttpJsonRpc,
                    Some(&chain),
                    native_call::call_method(&multistream, &method),
                    RequestStatus::Failed,
                    started.elapsed(),
                );
                protocol::build_error(
                    &id,
                    CODE_CALL_FAILURE,
                    format!("call task failed: {e}"),
                    None,
                )
            })
        })
    });
    let replies: ChunkStream = if preserve_order {
        // Still sends each reply once all the ones before it are done, rather
        // than waiting for the whole batch.
        Box::pin(calls.collect::<FuturesOrdered<_>>())
    } else {
        // Completion order is the default, since clients correlate by id anyway.
        Box::pin(calls.collect::<FuturesUnordered<_>>())
    };
    ResponseBody::Chunks(json_array(replies))
}

/// Frame the replies as a JSON array the way legacy `asArray` does: `[` up
/// front, each reply as it arrives with a comma before all but the first, and
/// `]` after the last.
fn json_array(replies: ChunkStream) -> ChunkStream {
    let items = replies.enumerate().map(
        |(i, reply)| {
            if i == 0 { reply } else { format!(",{reply}") }
        },
    );
    Box::pin(
        stream::once(async { "[".to_string() })
            .chain(items)
            .chain(stream::once(async { "]".to_string() })),
    )
}

/// What a call ended with, for the access log record.
struct CallOutcome {
    succeed: bool,
    status: RequestStatus,
    /// Response and error payloads, filled only under `include-messages`.
    response: Option<String>,
    error: Option<String>,
}

/// Execute one validated request and serialize its response object. Shared with
/// the WebSocket transport, which dispatches one validated call at a time.
/// `pos` is the call's position in its batch (0 for a single request).
pub(super) async fn run(
    multistream: &Multistream,
    protocol: DownstreamProtocol,
    chain: &TargetBlockchain,
    req: &ProxyRequest,
    access: Option<&AccessMeta>,
    pos: usize,
) -> String {
    let start = std::time::Instant::now();
    let (body, outcome) = execute(multistream, req).await;
    metrics::downstream_call(
        protocol,
        Some(chain),
        native_call::call_method(multistream, &req.method),
        outcome.status,
        start.elapsed(),
    );

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

async fn execute(multistream: &Multistream, req: &ProxyRequest) -> (String, CallOutcome) {
    let include = logs::access_enabled() && logs::include_messages();
    let method = match req.method.parse::<RpcMethod>() {
        Ok(method) => method,
        Err(_) => {
            // The transport never sees a malformed method name; report it the
            // way a node would rather than forwarding garbage.
            let body = protocol::build_error(&req.id, -32601, "Method not found".to_string(), None);
            return (
                body,
                CallOutcome {
                    succeed: false,
                    status: RequestStatus::Error,
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
    let routed = execute_call(multistream, &request, &LabelSelector::Any).await;
    let status = RequestStatus::from(routed.as_ref().map(|r| &r.response));
    match routed {
        Ok(routed) => {
            let resp = routed.response;
            if let Some(err) = resp.error {
                let outcome = CallOutcome {
                    succeed: false,
                    status,
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
                    status,
                    response: include.then(|| result.get().to_string()),
                    error: None,
                };
                (protocol::build_success(&req.id, &result), outcome)
            } else {
                let null = RawValue::from_string("null".to_string()).expect("valid json");
                let outcome = CallOutcome {
                    succeed: true,
                    status,
                    response: include.then(|| "null".to_string()),
                    error: None,
                };
                (protocol::build_success(&req.id, &null), outcome)
            }
        }
        Err(e) => {
            // An unsupported method keeps its distinct legacy code; every
            // other failure is a generic routing/quorum error.
            let code = match &e {
                UpstreamError::MethodNotAllowed(_) => protocol::CODE_METHOD_NOT_EXIST,
                _ => CODE_CALL_FAILURE,
            };
            let outcome = CallOutcome {
                succeed: false,
                status,
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

    fn request_context() -> Arc<logs::IngressContext> {
        Arc::new(logs::IngressContext::new(None))
    }

    /// The whole response text, however it was sent.
    async fn respond(body: &[u8], ms: Multistream, preserve_order: bool) -> String {
        let response = process(
            body,
            Arc::new(ms),
            chain(),
            preserve_order,
            request_context(),
            None,
        )
        .await;
        match response {
            ResponseBody::Complete(json) => json,
            ResponseBody::Chunks(chunks) => chunks.collect::<Vec<_>>().await.concat(),
        }
    }

    #[tokio::test]
    async fn single_request_echoes_id_and_result() {
        let ms = multistream(r#""0xabc""#);
        let body = respond(
            br#"{"jsonrpc":"2.0","id":42,"method":"eth_blockNumber","params":[]}"#,
            ms,
            false,
        )
        .await;
        assert_eq!(body, r#"{"jsonrpc":"2.0","id":42,"result":"0xabc"}"#);
    }

    #[tokio::test]
    async fn batch_returns_array_in_request_order() {
        let ms = multistream(r#""0x1""#);
        let body = respond(
            br#"[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]"#,
            ms,
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
        let body = respond(b"[]", ms, false).await;
        assert_eq!(body, "[]");
    }

    #[tokio::test]
    async fn parse_error_returns_single_object_with_fallback_id() {
        let ms = multistream(r#""0x1""#);
        let body = respond(b"not json", ms, false).await;
        assert_eq!(
            body,
            r#"{"jsonrpc":"2.0","id":-1,"error":{"code":-32700,"message":"Failed to parse JSON"}}"#
        );
    }

    #[tokio::test]
    async fn invalid_item_in_batch_aborts_whole_batch() {
        let ms = multistream(r#""0x1""#);
        // Second item is missing its id — the whole batch fails with one error.
        let body = respond(
            br#"[{"jsonrpc":"2.0","id":1,"method":"m"},{"jsonrpc":"2.0","method":"m"}]"#,
            ms,
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
        let body = respond(br#"{"jsonrpc":"2.0","id":9,"method":"eth_foo"}"#, ms, false).await;
        assert_eq!(
            body,
            r#"{"jsonrpc":"2.0","id":9,"error":{"code":-32601,"message":"Method not found"}}"#
        );
    }

    /// Upstream that answers after as many seconds as its first param, with
    /// that number as the result, so a batch can mix fast and slow calls.
    /// Counts the calls it finished, to tell a cancelled call from a done one.
    /// Panics when the param isn't a number, to stand for a crashed call.
    struct DelayUpstream {
        finished: AtomicUsize,
        state: Arc<UpstreamState>,
    }

    impl DelayUpstream {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                finished: AtomicUsize::new(0),
                state: Arc::new(UpstreamState::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for DelayUpstream {
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let Some(secs) = request.params[0].as_u64() else {
                panic!("upstream call crashed");
            };
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            self.finished.fetch_add(1, Ordering::SeqCst);
            let raw = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{secs}}}"#);
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

    /// A batch with a 60-second call first and a 1-second call second.
    const SLOW_THEN_FAST: &[u8] = br#"[{"jsonrpc":"2.0","id":"slow","method":"eth_blockNumber","params":[60]},{"jsonrpc":"2.0","id":"fast","method":"eth_blockNumber","params":[1]}]"#;

    fn batch_chunks(
        upstream: Arc<DelayUpstream>,
        body: &'static [u8],
        preserve_order: bool,
    ) -> impl Future<Output = ChunkStream> {
        let ms = Multistream::new(
            TargetBlockchain::Standard(emerald_api::proto::common::ChainRef::ChainEthereum),
            vec![upstream as Arc<dyn RpcUpstream>],
            Arc::new(AlwaysFactory),
        );
        async move {
            match process(
                body,
                Arc::new(ms),
                chain(),
                preserve_order,
                request_context(),
                None,
            )
            .await
            {
                ResponseBody::Chunks(chunks) => chunks,
                ResponseBody::Complete(json) => panic!("expected a streamed batch, got {json}"),
            }
        }
    }

    /// A fast call's reply goes out as soon as it's done, not after the slow
    /// call listed before it.
    #[tokio::test(start_paused = true)]
    async fn batch_sends_each_reply_when_ready() {
        let mut chunks = batch_chunks(DelayUpstream::new(), SLOW_THEN_FAST, false).await;
        let started = tokio::time::Instant::now();

        assert_eq!(chunks.next().await.unwrap(), "[");
        assert_eq!(
            chunks.next().await.unwrap(),
            r#"{"jsonrpc":"2.0","id":"fast","result":1}"#
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(60));
        assert_eq!(
            chunks.next().await.unwrap(),
            r#",{"jsonrpc":"2.0","id":"slow","result":60}"#
        );
        assert_eq!(chunks.next().await.unwrap(), "]");
        assert_eq!(chunks.next().await, None);
    }

    /// With the request order kept, a reply still goes out once the replies
    /// before it are done, without waiting for the rest of the batch.
    #[tokio::test(start_paused = true)]
    async fn ordered_batch_sends_each_reply_after_the_ones_before_it() {
        let body = br#"[{"jsonrpc":"2.0","id":"fast","method":"eth_blockNumber","params":[1]},{"jsonrpc":"2.0","id":"slow","method":"eth_blockNumber","params":[60]}]"#;
        let mut chunks = batch_chunks(DelayUpstream::new(), body, true).await;
        let started = tokio::time::Instant::now();

        assert_eq!(chunks.next().await.unwrap(), "[");
        assert_eq!(
            chunks.next().await.unwrap(),
            r#"{"jsonrpc":"2.0","id":"fast","result":1}"#
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(60));
        assert_eq!(
            chunks.next().await.unwrap(),
            r#",{"jsonrpc":"2.0","id":"slow","result":60}"#
        );
    }

    /// A call that crashes gets an error reply of its own; the rest of the
    /// batch is still answered.
    #[tokio::test(start_paused = true)]
    async fn crashed_batch_call_gets_error_reply() {
        let body = br#"[{"jsonrpc":"2.0","id":"ok","method":"eth_blockNumber","params":[1]},{"jsonrpc":"2.0","id":"crash","method":"eth_blockNumber","params":["x"]}]"#;
        let chunks = batch_chunks(DelayUpstream::new(), body, true).await;

        let replies = chunks.collect::<Vec<_>>().await.concat();

        let replies: serde_json::Value = serde_json::from_str(&replies).unwrap();
        assert_eq!(replies[0]["result"], 1);
        assert_eq!(replies[1]["id"], "crash");
        assert_eq!(replies[1]["error"]["code"], CODE_CALL_FAILURE);
    }

    /// A slow client reads the response late; the calls must not wait for it,
    /// or they'd run into their upstream timeouts.
    #[tokio::test(start_paused = true)]
    async fn batch_calls_complete_while_response_is_not_read() {
        let upstream = DelayUpstream::new();
        let chunks = batch_chunks(Arc::clone(&upstream), SLOW_THEN_FAST, false).await;

        tokio::time::sleep(std::time::Duration::from_secs(120)).await;

        assert_eq!(upstream.finished.load(Ordering::SeqCst), 2);
        drop(chunks);
    }

    /// A client that disconnects drops the response; the calls it was waiting
    /// for must stop with it rather than load the node for nobody.
    #[tokio::test(start_paused = true)]
    async fn dropping_batch_response_cancels_pending_calls() {
        let upstream = DelayUpstream::new();
        let mut chunks = batch_chunks(Arc::clone(&upstream), SLOW_THEN_FAST, false).await;

        assert_eq!(chunks.next().await.unwrap(), "[");
        chunks.next().await.unwrap();
        drop(chunks);
        tokio::time::sleep(std::time::Duration::from_secs(120)).await;

        assert_eq!(upstream.finished.load(Ordering::SeqCst), 1);
    }

    /// Batch calls run while the response is being sent, after `process` has
    /// returned, and must still be attributed to the client request.
    #[tokio::test]
    async fn batch_calls_run_in_request_context() {
        struct ContextProbe(Arc<UpstreamState>);
        #[async_trait::async_trait]
        impl RpcUpstream for ContextProbe {
            async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
                let raw = format!(
                    r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                    logs::current().is_some()
                );
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
                &self.0
            }
        }

        let ms = Multistream::new(
            TargetBlockchain::Standard(emerald_api::proto::common::ChainRef::ChainEthereum),
            vec![Arc::new(ContextProbe(Arc::new(UpstreamState::new()))) as Arc<dyn RpcUpstream>],
            Arc::new(AlwaysFactory),
        );
        let body = respond(
            br#"[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]"#,
            ms,
            true,
        )
        .await;
        assert_eq!(
            body,
            r#"[{"jsonrpc":"2.0","id":1,"result":true},{"jsonrpc":"2.0","id":2,"result":true}]"#
        );
    }
}
