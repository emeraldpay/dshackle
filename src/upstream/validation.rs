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

//! Active upstream validation.
//!
//! A background task periodically probes each upstream with chain-specific
//! checks (see [`EthereumValidator`](super::ethereum::validator::EthereumValidator)
//! and [`BitcoinValidator`](super::bitcoin::validator::BitcoinValidator)) and
//! records the verdict in the upstream's [`UpstreamState`](super::state).
//! Routing reads the combined state, so an upstream that is syncing, has too
//! few peers, serves the wrong chain, or simply doesn't respond stops
//! receiving traffic until a later round clears it.
//!
//! Probes are sent to the *transport* level of the upstream, below the
//! method-filter and cache wrappers — a user-configured method allow-list
//! must not be able to fail validation, and a cached answer must not pass it.

use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::traits::RpcUpstream;
use std::sync::Arc;
use std::time::Duration;

/// Chain-specific validation probes for a single upstream.
#[async_trait::async_trait]
pub trait UpstreamValidator: Send + Sync {
    /// Probe the upstream and report the availability it implies.
    async fn validate(&self, upstream: &dyn RpcUpstream) -> UpstreamAvailability;
}

/// Spawn the periodic validation loop for one upstream.
///
/// The first round runs immediately, so a broken upstream is taken out of
/// rotation at startup instead of one interval later.
pub fn start_validation(
    upstream: Arc<dyn RpcUpstream>,
    validator: Box<dyn UpstreamValidator>,
    interval: Duration,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let status = validator.validate(upstream.as_ref()).await;
            let state = upstream.state();
            if status != state.validation() {
                tracing::info!(
                    upstream = %upstream.id(),
                    %status,
                    "upstream validation status changed"
                );
            }
            state.set_validation(status);
        }
    });
}

/// Execute a single probe call and parse its result.
///
/// Returns `None` on any failure — timeout, transport error, JSON-RPC error,
/// or an unparseable result — which validators treat as the upstream being
/// unavailable. The timeout is essential: a hung connection must mark the
/// upstream as broken, not stall the validation loop.
pub(crate) async fn probe(
    upstream: &dyn RpcUpstream,
    method: &str,
    timeout: Duration,
) -> Option<serde_json::Value> {
    let request = JsonRpcRequest::new(0, method.into(), serde_json::json!([]));
    match tokio::time::timeout(timeout, upstream.call(&request)).await {
        Err(_) => {
            tracing::warn!(
                upstream = %upstream.id(),
                method,
                "no response to validation probe"
            );
            None
        }
        Ok(Err(e)) => {
            tracing::debug!(upstream = %upstream.id(), method, error = %e, "validation probe failed");
            None
        }
        Ok(Ok(response)) => {
            if let Some(error) = &response.error {
                tracing::debug!(upstream = %upstream.id(), method, %error, "validation probe rejected");
                return None;
            }
            let raw = response.result?;
            serde_json::from_str(raw.get()).ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::JsonRpcResponse;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::id::UpstreamId;
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::UpstreamError;

    /// Always answers with the configured availability.
    struct FixedValidator(UpstreamAvailability);

    #[async_trait::async_trait]
    impl UpstreamValidator for FixedValidator {
        async fn validate(&self, _: &dyn RpcUpstream) -> UpstreamAvailability {
            self.0
        }
    }

    struct StubUpstream {
        state: Arc<UpstreamState>,
    }

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            Err(UpstreamError::Transport("not used".into()))
        }
        fn id(&self) -> &UpstreamId {
            crate::upstream::id::stub_id()
        }
        fn availability(&self) -> UpstreamAvailability {
            self.state.availability()
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

    #[tokio::test]
    async fn loop_records_validation_result() {
        let upstream = Arc::new(StubUpstream {
            state: Arc::new(UpstreamState::new()),
        });

        start_validation(
            Arc::clone(&upstream) as Arc<dyn RpcUpstream>,
            Box::new(FixedValidator(UpstreamAvailability::Unavailable)),
            Duration::from_secs(3600),
        );

        // The first round runs immediately
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            upstream.state.availability(),
            UpstreamAvailability::Unavailable
        );
    }
}
