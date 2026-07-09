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

//! Connection loop for the proxy, plaintext or TLS-terminated.
//!
//! warp 0.4 ships no TLS server (its `tls` feature is an empty stub), so the
//! proxy accepts connections itself and drives the warp filter through hyper —
//! the same pipeline `warp::serve().run()` uses internally, plus an optional
//! rustls handshake in front.

use crate::tls::ServerTlsSetup;
use anyhow::{Context, Result};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower_service::Service;
use tracing;
use warp::Filter;

/// Remote peer address of the current connection, stored as a request
/// extension. `warp::addr::remote()` only works under `warp::serve`, which
/// this loop replaces, so proxy filters read this extension instead.
#[derive(Debug, Clone, Copy)]
pub struct PeerAddr(pub SocketAddr);

/// Accept connections on `addr` and serve the warp filter on each, upgrades
/// (WebSocket) included. With a TLS setup every connection is TLS-terminated
/// first. Runs until the listener fails.
pub async fn serve<F>(
    filter: F,
    addr: SocketAddr,
    tls: Option<&ServerTlsSetup>,
) -> Result<()>
where
    F: Filter<Extract = (warp::reply::Response,), Error = warp::Rejection>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let acceptor = match tls {
        Some(setup) => Some(TlsAcceptor::from(Arc::new(setup.rustls_config()?))),
        None => None,
    };
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding proxy to {addr}"))?;

    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::debug!("Proxy accept error: {e}");
                continue;
            }
        };
        let svc = warp::service(filter.clone());
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let hyper_svc = hyper::service::service_fn(
                move |mut req: hyper::Request<hyper::body::Incoming>| {
                    req.extensions_mut().insert(PeerAddr(peer));
                    let mut svc = svc.clone();
                    svc.call(req)
                },
            );
            let builder = auto::Builder::new(TokioExecutor::new());
            let served = match acceptor {
                Some(acceptor) => match acceptor.accept(tcp).await {
                    Ok(tls_stream) => {
                        builder
                            .serve_connection_with_upgrades(TokioIo::new(tls_stream), hyper_svc)
                            .await
                    }
                    Err(e) => {
                        tracing::debug!("TLS handshake failed from {peer}: {e}");
                        return;
                    }
                },
                None => {
                    builder
                        .serve_connection_with_upgrades(TokioIo::new(tcp), hyper_svc)
                        .await
                }
            };
            if let Err(e) = served {
                tracing::debug!("Proxy connection error from {peer}: {e:?}");
            }
        });
    }
}
