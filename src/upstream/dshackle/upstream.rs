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

//! Per-chain upstream backed by a remote Dshackle instance.
//!
//! Each `DshackleUpstream` represents a single blockchain on a remote Dshackle
//! server. RPC calls are forwarded via the `NativeCall` gRPC method, and head
//! tracking uses the `SubscribeHead` stream.

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::{CurrentHeight, Head};
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use emerald_api::proto::blockchain::blockchain_client::BlockchainClient;
use emerald_api::proto::blockchain::{NativeCallItem, NativeCallRequest};
use serde_json::value::RawValue;
use std::sync::Arc;
use tonic::transport::Channel;

/// A single-blockchain upstream backed by a remote Dshackle gRPC server.
///
/// Forwards JSON-RPC calls through `NativeCall`, which lets the remote server
/// route them to its own upstreams. The `BlockchainClient` is cloned per call —
/// tonic's `Channel` is designed to be cheaply cloned and shares the underlying
/// HTTP/2 connection pool.
pub struct DshackleUpstream {
    id: String,
    chain_ref: i32,
    client: BlockchainClient<Channel>,
    head: Arc<CurrentHeight>,
    state: Arc<UpstreamState>,
}

impl DshackleUpstream {
    /// Creates a new per-chain upstream backed by the given gRPC client.
    ///
    /// `chain_ref` is the protobuf `ChainRef` value used to route requests
    /// on the remote. `syncing_lag` sets the lag threshold for availability
    /// (6 for Ethereum-like chains, 2 for Bitcoin).
    pub fn new(
        id: String,
        chain_ref: i32,
        client: BlockchainClient<Channel>,
        syncing_lag: u64,
    ) -> Self {
        Self {
            id,
            chain_ref,
            client,
            head: Arc::new(CurrentHeight::new()),
            state: Arc::new(UpstreamState::with_syncing_lag(syncing_lag)),
        }
    }

    /// Shared reference to this upstream's head height, used by the head subscriber.
    pub fn head_height(&self) -> Arc<CurrentHeight> {
        Arc::clone(&self.head)
    }

    /// Returns the gRPC client handle (cheap clone) for spawning subscriptions.
    pub fn grpc_client(&self) -> BlockchainClient<Channel> {
        self.client.clone()
    }

}

#[async_trait::async_trait]
impl RpcUpstream for DshackleUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        tracing::trace!(upstream = %self.id, method = %request.method, "gRPC NativeCall");

        // Serialize the JSON-RPC params as the NativeCall payload
        let payload = serde_json::to_vec(&request.params)
            .map_err(|e| UpstreamError::Transport(format!("failed to serialize params: {e}")))?;

        let native_request = NativeCallRequest {
            chain: self.chain_ref,
            items: vec![NativeCallItem {
                id: request.id,
                method: request.method.to_string(),
                payload,
                nonce: 0,
            }],
            selector: None,
            quorum: 0,
            min_availability: 0,
        };

        let mut client = self.client.clone();
        let response = client
            .native_call(native_request)
            .await
            .map_err(|e| UpstreamError::Transport(format!("gRPC error: {e}")))?;

        let mut stream = response.into_inner();
        let reply = stream
            .message()
            .await
            .map_err(|e| UpstreamError::Transport(format!("gRPC stream error: {e}")))?
            .ok_or_else(|| {
                UpstreamError::InvalidResponse("empty NativeCall response stream".into())
            })?;

        if !reply.succeed {
            return Err(UpstreamError::InvalidResponse(
                if reply.error_message.is_empty() {
                    "remote call failed".into()
                } else {
                    reply.error_message
                },
            ));
        }

        // The reply payload is the raw JSON result value (e.g. `"0x1"` or `{...}`)
        let result = if reply.payload.is_empty() {
            None
        } else {
            let json_str = String::from_utf8(reply.payload).map_err(|e| {
                UpstreamError::InvalidResponse(format!("payload is not valid UTF-8: {e}"))
            })?;
            Some(
                RawValue::from_string(json_str).map_err(|e| {
                    UpstreamError::InvalidResponse(format!("payload is not valid JSON: {e}"))
                })?,
            )
        };

        Ok(JsonRpcResponse {
            id: serde_json::Value::from(request.id),
            result,
            error: None,
        })
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
