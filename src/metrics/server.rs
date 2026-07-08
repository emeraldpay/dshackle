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

//! HTTP endpoint serving the Prometheus scrape output.

use crate::config::monitoring::PrometheusConfig;
use prometheus::{Encoder, Registry, TextEncoder};
use std::net::{IpAddr, SocketAddr};
use warp::Filter;
use warp::http::Response;
use warp::path::FullPath;

/// Start the scrape endpoint in a background task. A failure to start (bad
/// address, port already in use) is logged but doesn't stop the application,
/// matching the legacy behavior — monitoring is auxiliary to the main service.
pub fn start(config: &PrometheusConfig, registry: &Registry) {
    let ip: IpAddr = match config.bind.parse() {
        Ok(ip) => ip,
        Err(_) => {
            tracing::error!(
                "Failed to start monitoring: invalid bind address {}",
                config.bind
            );
            return;
        }
    };
    let addr = SocketAddr::new(ip, config.port);
    let path = config.path.clone();
    let registry = registry.clone();
    tokio::spawn(async move {
        serve(addr, path, registry).await;
    });
}

async fn serve(addr: SocketAddr, path: String, registry: Registry) {
    // The configured path is an arbitrary multi-segment string, so match on
    // the full request path instead of warp's static segment filters.
    let route_path = path.clone();
    let route = warp::get()
        .and(warp::path::full())
        .and_then(move |full: FullPath| {
            let registry = registry.clone();
            let path = route_path.clone();
            async move {
                if full.as_str() != path {
                    return Err(warp::reject::not_found());
                }
                let encoder = TextEncoder::new();
                let mut buffer = Vec::new();
                if let Err(e) = encoder.encode(&registry.gather(), &mut buffer) {
                    tracing::warn!("Could not encode metrics: {}", e);
                }
                Ok(Response::builder()
                    .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                    .body(buffer))
            }
        });

    // Bind the listener explicitly: `warp::serve(...).run()` panics on a bind
    // failure, while monitoring must not take the main service down with it.
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("Failed to start monitoring on {}: {}", addr, e);
            return;
        }
    };
    tracing::info!("Prometheus metrics available at http://{}{}", addr, path);
    warp::serve(route).incoming(listener).run().await;
}
