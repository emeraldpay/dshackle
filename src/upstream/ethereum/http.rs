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

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::{CurrentHead, Head};
use crate::upstream::state::UpstreamState;
use crate::upstream::http_error::classify_non_200;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// An Ethereum upstream node accessed over HTTP JSON-RPC.
pub struct EthereumHttpUpstream {
    id: String,
    url: String,
    client: reqwest::Client,
    head: Arc<CurrentHead>,
    state: Arc<UpstreamState>,
}

impl EthereumHttpUpstream {
    pub fn new(id: String, url: String) -> Self {
        let client = reqwest::Client::new();
        let head = Arc::new(CurrentHead::new());
        let state = Arc::new(UpstreamState::new());
        Self {
            id,
            url,
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

        let resp = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .json(request)
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

    fn id(&self) -> &str {
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
