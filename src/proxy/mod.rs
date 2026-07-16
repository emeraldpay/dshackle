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

//! Standard JSON-RPC HTTP and WebSocket proxy.
//!
//! Exposes one endpoint per configured route, mapping the path to a blockchain
//! and serving JSON-RPC over the same call path as the gRPC `NativeCall`: `POST
//! /<route-id>` for HTTP, and a WebSocket upgrade on the same path. Ports the
//! legacy `proxy.ProxyServer` / `HttpHandler` / `WebsocketHandler`.

mod handler;
mod protocol;
mod serve;
mod ws;

use crate::blockchain::TargetBlockchain;
use crate::config::proxy::ProxyConfig;
use crate::logs;
use crate::tls::ServerTlsSetup;
use crate::upstream::egress::EgressSubscription;
use crate::upstream::{Multistream, UpstreamManager};
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use warp::Filter;
use warp::Reply;
use warp::http::HeaderMap;
use warp::http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE, HeaderValue,
};
use warp::http::{Method, StatusCode};

/// Default value for `Access-Control-Allow-Headers` when `cors-allowed-headers`
/// is not configured, matching the legacy default.
const DEFAULT_CORS_HEADERS: &str = "Content-Type";

/// CORS headers added to responses when `cors-origin` is configured.
struct Cors {
    origin: String,
    allowed_headers: String,
}

/// Resolved routing state shared with every request handler.
struct ProxyState {
    /// Route id (URL path segment, e.g. `eth`) → the chain's route.
    routes: HashMap<String, Arc<ProxyRoute>>,
    preserve_batch_order: bool,
    /// Whether WebSocket upgrades are served; when off, the path is HTTP-only.
    websocket: bool,
    /// CORS headers to emit, or `None` when `cors-origin` is unset.
    cors: Option<Cors>,
}

/// Everything one route needs: the upstreams for plain JSON-RPC calls and, when
/// the chain tracks a head, the egress for `eth_subscribe`. `egress` is `None`
/// for chains without a head — they answer calls but reject subscriptions.
struct ProxyRoute {
    chain: TargetBlockchain,
    multistream: Arc<Multistream>,
    egress: Option<Arc<dyn EgressSubscription>>,
}

/// Start the JSON-RPC HTTP proxy. Runs until the server stops; intended to be
/// spawned alongside the gRPC server.
///
/// With a TLS setup the proxy serves https/wss only; without one it serves
/// plaintext.
pub async fn start(
    config: &ProxyConfig,
    tls: Option<ServerTlsSetup>,
    upstreams: Arc<UpstreamManager>,
) -> anyhow::Result<()> {
    let routes = resolve_routes(config, &upstreams);
    if routes.is_empty() {
        tracing::warn!(
            "Proxy is enabled but has no usable routes; it will answer 404 for all paths"
        );
    }

    let cors = config.cors_origin.as_ref().map(|origin| Cors {
        origin: origin.clone(),
        allowed_headers: config
            .cors_allowed_headers
            .clone()
            .unwrap_or_else(|| DEFAULT_CORS_HEADERS.to_string()),
    });

    let state = Arc::new(ProxyState {
        routes,
        preserve_batch_order: config.preserve_batch_order,
        websocket: config.websocket,
        cors,
    });

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    let scheme = if tls.is_some() { "https" } else { "http" };
    tracing::info!("JSON-RPC HTTP proxy listening on {scheme}://{addr}");
    serve::serve(routes_filter(state), addr, tls.as_ref()).await
}

/// Remote peer address, extracted from the connection loop's request
/// extension (see [`serve::PeerAddr`]).
fn peer_addr()
-> impl Filter<Extract = (Option<SocketAddr>,), Error = std::convert::Infallible> + Copy {
    warp::ext::optional::<serve::PeerAddr>().map(|peer: Option<serve::PeerAddr>| peer.map(|p| p.0))
}

/// Build the request filter: a single exactly-matched segment (`/eth`, not
/// `/eth/x`). A WebSocket upgrade on the path is served as a JSON-RPC WS
/// connection; everything else dispatches by HTTP method inside the handler.
/// Matching all methods on the path keeps the codes deterministic — unknown
/// route → 404, known route with the wrong method → 405 — matching the legacy
/// exact routes.
fn routes_filter(
    state: Arc<ProxyState>,
) -> impl Filter<Extract = (warp::reply::Response,), Error = warp::Rejection> + Clone {
    let ws_state = Arc::clone(&state);
    // `warp::ws()` matches only genuine upgrade requests, so plain POST/OPTIONS
    // fall through to the HTTP branch.
    let ws_route = warp::path::param::<String>()
        .and(warp::path::end())
        .and(warp::ws())
        .and(peer_addr())
        .and(warp::header::headers_cloned())
        .and(warp::any().map(move || Arc::clone(&ws_state)))
        .map(
            |segment: String,
             upgrade: warp::ws::Ws,
             peer: Option<SocketAddr>,
             headers: HeaderMap,
             state: Arc<ProxyState>| {
                if !state.websocket {
                    // WS disabled: the path only serves POST/OPTIONS, so the
                    // upgrade (a GET) is not allowed here.
                    return warp::reply::with_status(String::new(), StatusCode::METHOD_NOT_ALLOWED)
                        .into_response();
                }
                match state.routes.get(&segment) {
                    Some(route) => {
                        let route = Arc::clone(route);
                        // The whole connection is one client request in the
                        // access log: every call and subscription reply on it
                        // shares the identity created at the upgrade.
                        let ctx = Arc::new(logs::IngressContext::new(Some(remote_details(
                            peer, &headers,
                        ))));
                        upgrade
                            .on_upgrade(move |socket| ws::serve(socket, route, ctx))
                            .into_response()
                    }
                    None => warp::reply::with_status(String::new(), StatusCode::NOT_FOUND)
                        .into_response(),
                }
            },
        );

    let http_route = warp::path::param::<String>()
        .and(warp::path::end())
        .and(warp::method())
        .and(warp::body::bytes())
        .and(peer_addr())
        .and(warp::header::headers_cloned())
        .and(warp::any().map(move || Arc::clone(&state)))
        .then(handle);

    ws_route.or(http_route).unify()
}

/// Who is making the request, from the transport peer and forwarding headers.
fn remote_details(peer: Option<SocketAddr>, headers: &HeaderMap) -> logs::Remote {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    logs::Remote::from_request(
        peer.map(|addr| addr.ip()),
        header("x-real-ip"),
        header("x-forwarded-for"),
        header("user-agent"),
    )
}

/// Build the path → route map, skipping routes whose blockchain is unknown or
/// has no configured upstreams.
fn resolve_routes(
    config: &ProxyConfig,
    upstreams: &UpstreamManager,
) -> HashMap<String, Arc<ProxyRoute>> {
    let mut routes = HashMap::new();
    for route in &config.routes {
        let chain: TargetBlockchain = match route.blockchain.parse() {
            Ok(chain) => chain,
            Err(_) => {
                tracing::warn!(
                    "Proxy route '/{}' has unknown blockchain '{}', skipping",
                    route.id,
                    route.blockchain
                );
                continue;
            }
        };
        match upstreams.get(&chain) {
            Some(multistream) => {
                tracing::info!("Proxy route '/{}' -> {}", route.id, chain);
                routes.insert(
                    route.id.clone(),
                    Arc::new(ProxyRoute {
                        chain,
                        multistream: Arc::clone(multistream),
                        egress: upstreams.egress(&chain),
                    }),
                );
            }
            None => tracing::warn!(
                "Proxy route '/{}' -> {} has no upstreams, skipping",
                route.id,
                chain
            ),
        }
    }
    routes
}

async fn handle(
    segment: String,
    method: Method,
    body: Bytes,
    peer: Option<SocketAddr>,
    headers: HeaderMap,
    state: Arc<ProxyState>,
) -> warp::reply::Response {
    let Some(route) = state.routes.get(&segment) else {
        // Unknown route, any method.
        return warp::reply::with_status(String::new(), StatusCode::NOT_FOUND).into_response();
    };

    match method {
        Method::POST => {
            let ctx = Arc::new(logs::IngressContext::new(Some(remote_details(
                peer, &headers,
            ))));
            let access = handler::AccessMeta::new(logs::Channel::JsonRpc, Arc::clone(&ctx));
            // The context makes the upstream calls attributable to this
            // client request in the request log.
            let body = logs::with_context(
                (*ctx).clone(),
                handler::process(
                    &body,
                    &route.multistream,
                    &route.chain,
                    state.preserve_batch_order,
                    access,
                ),
            )
            .await;
            // Always HTTP 200; JSON-RPC errors are carried in the body.
            let mut response = warp::reply::with_status(body, StatusCode::OK).into_response();
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            add_cors_headers(&mut response, &state);
            response
        }
        // CORS preflight: 200, empty body, CORS headers when configured.
        Method::OPTIONS => {
            let mut response =
                warp::reply::with_status(String::new(), StatusCode::OK).into_response();
            add_cors_headers(&mut response, &state);
            response
        }
        _ => {
            warp::reply::with_status(String::new(), StatusCode::METHOD_NOT_ALLOWED).into_response()
        }
    }
}

/// Add `Access-Control-Allow-Origin` / `Access-Control-Allow-Headers` when CORS
/// is configured. A header value that can't be encoded is skipped rather than
/// failing the request.
fn add_cors_headers(response: &mut warp::reply::Response, state: &ProxyState) {
    let Some(cors) = &state.cors else {
        return;
    };
    let headers = response.headers_mut();
    if let Ok(origin) = HeaderValue::from_str(&cors.origin) {
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    if let Ok(allowed) = HeaderValue::from_str(&cors.allowed_headers) {
        headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, allowed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::id::UpstreamId;
    use crate::upstream::quorum::{AlwaysQuorum, CallQuorum, QuorumFactory};
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::{RpcUpstream, UpstreamError};

    struct StubUpstream(Arc<UpstreamState>);
    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            Ok(serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#).unwrap())
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

    struct AlwaysFactory;
    impl QuorumFactory for AlwaysFactory {
        fn quorum_for(&self, _method: &RpcMethod) -> Box<dyn CallQuorum> {
            Box::new(AlwaysQuorum::new())
        }
    }

    fn stub_multistream() -> Arc<Multistream> {
        let upstream: Arc<dyn RpcUpstream> = Arc::new(StubUpstream(Arc::new(UpstreamState::new())));
        Arc::new(Multistream::new(
            TargetBlockchain::Standard(emerald_api::proto::common::ChainRef::ChainEthereum),
            vec![upstream],
            Arc::new(AlwaysFactory),
        ))
    }

    fn state_with(cors: Option<Cors>) -> Arc<ProxyState> {
        let mut routes = HashMap::new();
        routes.insert(
            "eth".to_string(),
            Arc::new(ProxyRoute {
                chain: "ethereum".parse().unwrap(),
                multistream: stub_multistream(),
                egress: None,
            }),
        );
        Arc::new(ProxyState {
            routes,
            preserve_batch_order: false,
            websocket: true,
            cors,
        })
    }

    fn state_with_eth_route() -> Arc<ProxyState> {
        state_with(None)
    }

    #[tokio::test]
    async fn post_known_route_returns_json_200() {
        let filter = routes_filter(state_with_eth_route());
        let resp = warp::test::request()
            .method("POST")
            .path("/eth")
            .body(r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}"#)
            .reply(&filter)
            .await;
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers()["content-type"], "application/json");
        assert_eq!(
            resp.body(),
            &b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}"[..]
        );
    }

    #[tokio::test]
    async fn unknown_route_is_404() {
        let filter = routes_filter(state_with_eth_route());
        let resp = warp::test::request()
            .method("POST")
            .path("/nope")
            .body("{}")
            .reply(&filter)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn nested_path_does_not_match() {
        let filter = routes_filter(state_with_eth_route());
        let resp = warp::test::request()
            .method("POST")
            .path("/eth/extra")
            .body("{}")
            .reply(&filter)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn get_method_is_rejected() {
        let filter = routes_filter(state_with_eth_route());
        let resp = warp::test::request()
            .method("GET")
            .path("/eth")
            .reply(&filter)
            .await;
        // warp rejects the non-POST method (405), never reaching the handler.
        assert_eq!(resp.status(), 405);
    }

    fn cors() -> Option<Cors> {
        Some(Cors {
            origin: "*".to_string(),
            allowed_headers: DEFAULT_CORS_HEADERS.to_string(),
        })
    }

    #[tokio::test]
    async fn post_includes_cors_headers_when_configured() {
        let filter = routes_filter(state_with(cors()));
        let resp = warp::test::request()
            .method("POST")
            .path("/eth")
            .body(r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}"#)
            .reply(&filter)
            .await;
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers()["access-control-allow-origin"], "*");
        assert_eq!(
            resp.headers()["access-control-allow-headers"],
            "Content-Type"
        );
    }

    #[tokio::test]
    async fn post_has_no_cors_headers_when_unset() {
        let filter = routes_filter(state_with_eth_route());
        let resp = warp::test::request()
            .method("POST")
            .path("/eth")
            .body(r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}"#)
            .reply(&filter)
            .await;
        assert!(!resp.headers().contains_key("access-control-allow-origin"));
    }

    #[tokio::test]
    async fn options_preflight_returns_200_with_cors_headers() {
        let filter = routes_filter(state_with(cors()));
        let resp = warp::test::request()
            .method("OPTIONS")
            .path("/eth")
            .reply(&filter)
            .await;
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers()["access-control-allow-origin"], "*");
        assert!(resp.body().is_empty());
    }

    #[tokio::test]
    async fn options_unknown_route_is_404() {
        let filter = routes_filter(state_with(cors()));
        let resp = warp::test::request()
            .method("OPTIONS")
            .path("/nope")
            .reply(&filter)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn ws_plain_call_returns_result() {
        let filter = routes_filter(state_with_eth_route());
        let mut client = warp::test::ws()
            .path("/eth")
            .handshake(filter)
            .await
            .expect("handshake succeeds");
        client
            .send_text(r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}"#)
            .await;
        let msg = client.recv().await.expect("a response frame");
        assert_eq!(
            msg.to_str().unwrap(),
            r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#
        );
    }

    #[tokio::test]
    async fn ws_subscribe_rejected_when_chain_has_no_egress() {
        // A route without a head can't serve subscriptions.
        let filter = routes_filter(state_with_eth_route());
        let mut client = warp::test::ws()
            .path("/eth")
            .handshake(filter)
            .await
            .expect("handshake succeeds");
        client
            .send_text(r#"{"jsonrpc":"2.0","id":7,"method":"eth_subscribe","params":["newHeads"]}"#)
            .await;
        let msg = client.recv().await.expect("a response frame");
        let value: serde_json::Value = serde_json::from_str(msg.to_str().unwrap()).unwrap();
        assert_eq!(value["id"], 7);
        assert_eq!(value["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn ws_ignores_batch_and_answers_next_single() {
        let filter = routes_filter(state_with_eth_route());
        let mut client = warp::test::ws()
            .path("/eth")
            .handshake(filter)
            .await
            .expect("handshake succeeds");
        // A batch is silently dropped; the following single still gets answered.
        client
            .send_text(r#"[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}]"#)
            .await;
        client
            .send_text(r#"{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}"#)
            .await;
        let msg = client.recv().await.expect("a response frame");
        let value: serde_json::Value = serde_json::from_str(msg.to_str().unwrap()).unwrap();
        assert_eq!(value["id"], 2);
    }

    #[tokio::test]
    async fn ws_rejected_when_disabled() {
        let mut state = state_with_eth_route();
        // Rebuild with websocket off.
        Arc::get_mut(&mut state).unwrap().websocket = false;
        let filter = routes_filter(state);
        let result = warp::test::ws().path("/eth").handshake(filter).await;
        assert!(result.is_err(), "upgrade must fail when WS is disabled");
    }

    /// A route whose chain has a working `newHeads` egress, plus the head to
    /// feed blocks into it.
    fn state_with_egress() -> (Arc<ProxyState>, Arc<crate::upstream::head::CurrentHead>) {
        use crate::upstream::egress::{ChainAccess, EthereumEgress};
        use crate::upstream::head::CurrentHead;
        use crate::upstream::merged_head::MergedHead;

        let multistream = stub_multistream();
        let head = Arc::new(CurrentHead::new());
        let merged = MergedHead::new(vec![Arc::clone(&head)]);
        let access: Arc<dyn ChainAccess> = multistream.clone();
        let egress: Arc<dyn EgressSubscription> = Arc::new(EthereumEgress::new(merged, access));

        let mut routes = HashMap::new();
        routes.insert(
            "eth".to_string(),
            Arc::new(ProxyRoute {
                chain: "ethereum".parse().unwrap(),
                multistream,
                egress: Some(egress),
            }),
        );
        let state = Arc::new(ProxyState {
            routes,
            preserve_batch_order: false,
            websocket: true,
            cors: None,
        });
        (state, head)
    }

    fn head_block(height: u64) -> crate::data::BlockContainer {
        use crate::data::{BlockContainer, BlockId};
        let mut hash = [0u8; 32];
        hash[0] = height as u8;
        let header =
            format!(r#"{{"number":"0x{height:x}","gasLimit":"0x1c9c380","difficulty":"0x0"}}"#);
        BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: Some(BlockId::from_bytes([0xbb; 32])),
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: Some(Arc::from(header.into_bytes().as_slice())),
        }
    }

    #[tokio::test]
    async fn ws_newheads_subscription_streams_blocks() {
        let (state, head) = state_with_egress();
        let filter = routes_filter(state);
        let mut client = warp::test::ws()
            .path("/eth")
            .handshake(filter)
            .await
            .expect("handshake succeeds");

        client
            .send_text(r#"{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}"#)
            .await;

        // First frame carries the subscription id.
        let first: serde_json::Value =
            serde_json::from_str(client.recv().await.unwrap().to_str().unwrap()).unwrap();
        assert_eq!(first["id"], 1);
        let sub_id = first["result"]
            .as_str()
            .expect("subscription id")
            .to_string();

        // A new head is pushed as an eth_subscription envelope.
        head.update_with_block(head_block(0x42));
        let event: serde_json::Value =
            serde_json::from_str(client.recv().await.unwrap().to_str().unwrap()).unwrap();
        assert_eq!(event["method"], "eth_subscription");
        assert_eq!(event["params"]["subscription"], sub_id);
        assert_eq!(event["params"]["result"]["number"], "0x42");
        assert_eq!(event["params"]["result"]["gasLimit"], "0x1c9c380");
    }

    #[tokio::test]
    async fn ws_unsubscribe_reports_active_then_unknown() {
        let (state, _head) = state_with_egress();
        let filter = routes_filter(state);
        let mut client = warp::test::ws()
            .path("/eth")
            .handshake(filter)
            .await
            .expect("handshake succeeds");

        client
            .send_text(r#"{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}"#)
            .await;
        let first: serde_json::Value =
            serde_json::from_str(client.recv().await.unwrap().to_str().unwrap()).unwrap();
        let sub_id = first["result"].as_str().unwrap().to_string();

        // Unsubscribing an active subscription returns true.
        client
            .send_text(&format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"eth_unsubscribe","params":["{sub_id}"]}}"#
            ))
            .await;
        let ok: serde_json::Value =
            serde_json::from_str(client.recv().await.unwrap().to_str().unwrap()).unwrap();
        assert_eq!(ok["id"], 2);
        assert_eq!(ok["result"], true);

        // Unsubscribing an unknown id returns false.
        client
            .send_text(
                r#"{"jsonrpc":"2.0","id":3,"method":"eth_unsubscribe","params":["deadbeef"]}"#,
            )
            .await;
        let unknown: serde_json::Value =
            serde_json::from_str(client.recv().await.unwrap().to_str().unwrap()).unwrap();
        assert_eq!(unknown["result"], false);
    }
}
