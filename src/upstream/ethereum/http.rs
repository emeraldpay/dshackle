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
use crate::upstream::traits::{RpcUpstream, UpstreamError};

/// An Ethereum upstream node accessed over HTTP JSON-RPC.
pub struct EthereumHttpUpstream {
    #[allow(dead_code)]
    id: String,
    url: String,
    client: reqwest::Client,
}

impl EthereumHttpUpstream {
    pub fn new(id: String, url: String) -> Self {
        let client = reqwest::Client::new();
        Self { id, url, client }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for EthereumHttpUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
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
            return Err(UpstreamError::HttpStatus(status));
        }

        let body = resp
            .bytes()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?;

        serde_json::from_slice::<JsonRpcResponse>(&body)
            .map_err(|e| UpstreamError::InvalidResponse(e.to_string()))
    }
}
