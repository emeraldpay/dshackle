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

//! Ethereum upstream that communicates via HTTP JSON-RPC.

use crate::config::tls::BasicAuth;
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::{CurrentHead, Head};
use crate::upstream::http_error::classify_non_200;
use crate::upstream::id::UpstreamId;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// An Ethereum upstream node accessed over HTTP JSON-RPC.
pub struct EthereumHttpUpstream {
    id: UpstreamId,
    url: String,
    basic_auth: Option<BasicAuth>,
    client: reqwest::Client,
    head: Arc<CurrentHead>,
    state: Arc<UpstreamState>,
}

impl EthereumHttpUpstream {
    /// `client` carries the transport-level options decided at wiring time
    /// (custom CA, mutual TLS). If `basic_auth` is provided it is applied to
    /// every outgoing request.
    pub fn new(
        id: UpstreamId,
        url: String,
        basic_auth: Option<BasicAuth>,
        client: reqwest::Client,
    ) -> Self {
        let head = Arc::new(CurrentHead::new());
        let state = Arc::new(UpstreamState::new());
        Self {
            id,
            url,
            basic_auth,
            client,
            head,
            state,
        }
    }

    /// Shared reference to this upstream's head height, used to start the poller.
    pub fn head_height(&self) -> Arc<CurrentHead> {
        Arc::clone(&self.head)
    }
}

#[async_trait::async_trait]
impl RpcUpstream for EthereumHttpUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        tracing::trace!(upstream = %self.id, method = %request.method, "HTTP request");

        let mut req_builder = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .json(request);

        if let Some(auth) = &self.basic_auth {
            req_builder = req_builder.basic_auth(&auth.username, Some(&auth.password));
        }

        let resp = req_builder
            .send()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?;

        let status = resp.status().as_u16();
        if status != 200 {
            let body = resp.text().await.unwrap_or_default();
            return Err(classify_non_200(&self.id, &self.state, status, &body));
        }

        let body = resp
            .bytes()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?;

        tracing::trace!(upstream = %self.id, bytes = body.len(), "HTTP response received");

        serde_json::from_slice::<JsonRpcResponse>(&body)
            .map_err(|e| UpstreamError::InvalidResponse(e.to_string()))
    }

    fn id(&self) -> &UpstreamId {
        &self.id
    }

    fn availability(&self) -> UpstreamAvailability {
        self.state.availability()
    }

    fn head(&self) -> &dyn Head {
        self.head.as_ref()
    }

    fn lag(&self) -> Option<u64> {
        self.state.lag()
    }

    fn state(&self) -> &Arc<UpstreamState> {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A connected-but-silent upstream must fail the call at the client's
    /// timeout (`options.timeout` at wiring) instead of hanging the request —
    /// the router needs the error to fall through to the next candidate.
    #[tokio::test]
    async fn call_times_out_on_a_silent_upstream() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Accept connections and hold them open without ever responding.
        tokio::spawn(async move {
            let mut open = Vec::new();
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                open.push(socket);
            }
        });

        let client =
            crate::tls::reqwest_client(None, std::time::Duration::from_millis(300), false).unwrap();
        let upstream = EthereumHttpUpstream::new(
            "silent".parse().unwrap(),
            format!("http://{addr}/"),
            None,
            client,
        );

        let request = JsonRpcRequest::new(1, "eth_blockNumber".into(), serde_json::json!([]));
        let started = std::time::Instant::now();
        let result = upstream.call(&request).await;

        assert!(matches!(result, Err(UpstreamError::Transport(_))));
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }
}
