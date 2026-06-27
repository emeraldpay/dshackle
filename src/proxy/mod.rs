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

//! Standard JSON-RPC HTTP proxy.
//!
//! Exposes one `POST /<route-id>` endpoint per configured route, mapping the
//! path to a blockchain and serving JSON-RPC over the same call path as the
//! gRPC `NativeCall`. Ports the legacy `proxy.ProxyServer` / `HttpHandler`.

mod handler;
mod protocol;

use crate::blockchain::TargetBlockchain;
use crate::config::proxy::ProxyConfig;
use crate::upstream::{Multistream, UpstreamManager};
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use warp::Filter;
use warp::Reply;
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
    /// Route id (URL path segment, e.g. `eth`) → the chain's upstreams.
    routes: HashMap<String, Arc<Multistream>>,
    preserve_batch_order: bool,
    /// CORS headers to emit, or `None` when `cors-origin` is unset.
    cors: Option<Cors>,
}

/// Start the JSON-RPC HTTP proxy. Runs until the server stops; intended to be
/// spawned alongside the gRPC server.
pub async fn start(config: &ProxyConfig, upstreams: Arc<UpstreamManager>) -> anyhow::Result<()> {
    warn_unsupported(config);

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
        cors,
    });

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    tracing::info!("JSON-RPC HTTP proxy listening on {}", addr);
    warp::serve(routes_filter(state)).run(addr).await;
    Ok(())
}

/// Build the request filter: a single exactly-matched segment (`/eth`, not
/// `/eth/x`), with the HTTP method dispatched inside the handler. Matching all
/// methods on the path keeps the codes deterministic — unknown route → 404,
/// known route with the wrong method → 405 — matching the legacy exact routes.
fn routes_filter(
    state: Arc<ProxyState>,
) -> impl Filter<Extract = (warp::reply::Response,), Error = warp::Rejection> + Clone {
    warp::path::param::<String>()
        .and(warp::path::end())
        .and(warp::method())
        .and(warp::body::bytes())
        .and(warp::any().map(move || Arc::clone(&state)))
        .then(handle)
}

/// Build the path → upstreams map, skipping routes whose blockchain is unknown
/// or has no configured upstreams.
fn resolve_routes(
    config: &ProxyConfig,
    upstreams: &UpstreamManager,
) -> HashMap<String, Arc<Multistream>> {
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
                routes.insert(route.id.clone(), Arc::clone(multistream));
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
    state: Arc<ProxyState>,
) -> warp::reply::Response {
    let Some(multistream) = state.routes.get(&segment) else {
        // Unknown route, any method.
        return warp::reply::with_status(String::new(), StatusCode::NOT_FOUND).into_response();
    };

    match method {
        Method::POST => {
            let body = handler::process(&body, multistream, state.preserve_batch_order).await;
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

/// Warn loudly about config sections that are parsed but not yet honored, so
/// operators aren't surprised by silently-ignored settings.
fn warn_unsupported(config: &ProxyConfig) {
    if config.tls.is_some() {
        tracing::warn!("Proxy TLS is configured but not yet supported; serving plaintext HTTP");
    }
    if config.websocket {
        tracing::warn!(
            "Proxy WebSocket is enabled but not yet supported; only HTTP POST is served"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::quorum::{AlwaysQuorum, CallQuorum, QuorumFactory};
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::{RpcUpstream, UpstreamError};

    struct StubUpstream(Arc<UpstreamState>);
    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            Ok(serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#).unwrap())
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
            &self.0
        }
    }

    struct AlwaysFactory;
    impl QuorumFactory for AlwaysFactory {
        fn quorum_for(&self, _method: &RpcMethod) -> Box<dyn CallQuorum> {
            Box::new(AlwaysQuorum::new())
        }
    }

    fn state_with(cors: Option<Cors>) -> Arc<ProxyState> {
        let upstream: Arc<dyn RpcUpstream> = Arc::new(StubUpstream(Arc::new(UpstreamState::new())));
        let multistream = Arc::new(Multistream::new(vec![upstream], Arc::new(AlwaysFactory)));
        let mut routes = HashMap::new();
        routes.insert("eth".to_string(), multistream);
        Arc::new(ProxyState {
            routes,
            preserve_batch_order: false,
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
}
