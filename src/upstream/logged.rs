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

//! Wrapper that writes a request log record for every call to an upstream
//! transport — the equivalent of the legacy `LoggingJsonRpcReader`.
//!
//! Installed directly above the transport, next to the metrics wrapper, so
//! only requests that actually reach the upstream are logged.

use crate::blockchain::TargetBlockchain;
use crate::config::upstreams::UpstreamRole;
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::logs;
use crate::logs::record::{Channel, LogTimestamp};
use crate::logs::request::{
    ErrorDetails, IngressRequestDetails, JsonRpcDetails, RequestRecord, Source, UpstreamDetails,
    abbreviated_params, legacy_elapsed_ms,
};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{Capability, RpcUpstream, UpstreamError};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Decorates an upstream transport with request logging, delegating all
/// request handling to the inner upstream unchanged.
pub struct LoggedUpstream {
    inner: Arc<dyn RpcUpstream>,
    /// The `upstream.id` of the records: the configured upstream id.
    upstream: String,
    channel: Channel,
    chain: TargetBlockchain,
}

impl LoggedUpstream {
    pub fn new(
        inner: Arc<dyn RpcUpstream>,
        channel: Channel,
        upstream: String,
        chain: TargetBlockchain,
    ) -> Self {
        Self {
            inner,
            upstream,
            channel,
            chain,
        }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for LoggedUpstream {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        if !logs::request_enabled() {
            return self.inner.call(request).await;
        }

        // The client request being served, or an internal origin (head polls,
        // validation probes) with its own identity.
        let ingress = match logs::current() {
            Some(ctx) => IngressRequestDetails {
                source: Source::Request,
                id: ctx.id,
                start: ctx.start,
            },
            None => IngressRequestDetails {
                source: Source::Internal,
                id: Uuid::new_v4(),
                start: LogTimestamp::now(),
            },
        };

        let execute = LogTimestamp::now();
        let result = self.inner.call(request).await;
        let complete = LogTimestamp::now();

        let (success, response_size, error) = match &result {
            Ok(response) => match &response.error {
                Some(err) => (
                    false,
                    0,
                    Some(ErrorDetails {
                        code: err.code,
                        message: Some(err.message.clone()),
                    }),
                ),
                None => (
                    true,
                    response.result.as_ref().map(|r| r.get().len()).unwrap_or(0),
                    None,
                ),
            },
            // A non-JSON-RPC failure is reported with code 0 and the rendered
            // message, like the legacy catch-all error mapping.
            Err(e) => (
                false,
                0,
                Some(ErrorDetails {
                    code: 0,
                    message: Some(format!("ERROR {e}")),
                }),
            ),
        };

        let queue_time = legacy_elapsed_ms(&ingress.start, &execute);
        let record = RequestRecord {
            version: logs::request::VERSION,
            id: Uuid::new_v4(),
            success,
            upstream: UpstreamDetails {
                id: self.upstream.clone(),
                channel: self.channel,
                request_type: "JSONRPC",
            },
            request: ingress,
            jsonrpc: JsonRpcDetails {
                method: request.method.as_ref().to_string(),
                params: logs::include_params().then(|| abbreviated_params(&request.params)),
                id: request.id as i64,
            },
            blockchain: self.chain.legacy_name(),
            execute: Some(execute),
            complete,
            response_size,
            error,
            queue_time,
            request_time: Some(legacy_elapsed_ms(&execute, &complete)),
        };
        logs::request_log(&record);

        result
    }

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn availability(&self) -> UpstreamAvailability {
        self.inner.availability()
    }

    fn head(&self) -> &dyn Head {
        self.inner.head()
    }

    fn lag(&self) -> Option<u64> {
        self.inner.lag()
    }

    fn state(&self) -> &Arc<UpstreamState> {
        self.inner.state()
    }

    fn allows_method(&self, method: &RpcMethod) -> bool {
        self.inner.allows_method(method)
    }

    fn label_sets(&self) -> &[HashMap<String, String>] {
        self.inner.label_sets()
    }

    fn role(&self) -> UpstreamRole {
        self.inner.role()
    }

    fn capabilities(&self) -> Vec<Capability> {
        self.inner.capabilities()
    }
}
