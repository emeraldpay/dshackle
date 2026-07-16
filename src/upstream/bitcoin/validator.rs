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

//! Bitcoin upstream validation, the port of the legacy
//! `BitcoinUpstreamValidator`.
//!
//! Bitcoin Core has no `eth_syncing` analog worth probing (initial sync
//! already shows up as a huge lag), so the only check is the peer count via
//! `getconnectioncount`. As with Ethereum, any probe failure makes the
//! upstream `Unavailable`.

use crate::config::upstreams::Options;
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::traits::RpcUpstream;
use crate::upstream::validation::{UpstreamValidator, probe};
use std::time::Duration;

pub struct BitcoinValidator {
    options: Options,
}

impl BitcoinValidator {
    pub fn new(options: Options) -> Self {
        Self { options }
    }

    /// A probe must answer well before the overall request timeout, matching
    /// the legacy `timeoutInternal = timeout / 4`.
    fn probe_timeout(&self) -> Duration {
        self.options.timeout / 4
    }
}

#[async_trait::async_trait]
impl UpstreamValidator for BitcoinValidator {
    async fn validate(&self, upstream: &dyn RpcUpstream) -> UpstreamAvailability {
        if !self.options.validate_peers || self.options.min_peers == 0 {
            return UpstreamAvailability::Ok;
        }
        let count = probe(upstream, "getconnectioncount", self.probe_timeout())
            .await
            .and_then(|v| v.as_u64());
        match count {
            Some(count) if count < self.options.min_peers as u64 => UpstreamAvailability::Immature,
            Some(_) => UpstreamAvailability::Ok,
            None => UpstreamAvailability::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::PartialOptions;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::id::UpstreamId;
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::UpstreamError;
    use std::sync::Arc;

    static MOCK_STATE: std::sync::LazyLock<Arc<UpstreamState>> =
        std::sync::LazyLock::new(|| Arc::new(UpstreamState::new()));

    /// Answers `getconnectioncount` with the given count, or fails when `None`.
    struct StubUpstream(Option<u64>);

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            match self.0 {
                Some(count) => {
                    let raw = format!(r#"{{"jsonrpc":"2.0","id":0,"result":{count}}}"#);
                    Ok(serde_json::from_str(&raw).unwrap())
                }
                None => Err(UpstreamError::Transport("refused".into())),
            }
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
            &MOCK_STATE
        }
    }

    fn validator(partial: PartialOptions) -> BitcoinValidator {
        BitcoinValidator::new(partial.build())
    }

    #[tokio::test]
    async fn enough_peers_is_ok() {
        let v = validator(PartialOptions::default());
        assert_eq!(
            v.validate(&StubUpstream(Some(8))).await,
            UpstreamAvailability::Ok
        );
    }

    #[tokio::test]
    async fn too_few_peers_is_immature() {
        let v = validator(PartialOptions {
            min_peers: Some(5),
            ..Default::default()
        });
        assert_eq!(
            v.validate(&StubUpstream(Some(2))).await,
            UpstreamAvailability::Immature
        );
    }

    #[tokio::test]
    async fn connection_error_is_unavailable() {
        let v = validator(PartialOptions::default());
        assert_eq!(
            v.validate(&StubUpstream(None)).await,
            UpstreamAvailability::Unavailable
        );
    }

    #[tokio::test]
    async fn peers_check_can_be_disabled() {
        let v = validator(PartialOptions {
            validate_peers: Some(false),
            ..Default::default()
        });
        // Even a dead upstream passes when the only check is off
        assert_eq!(
            v.validate(&StubUpstream(None)).await,
            UpstreamAvailability::Ok
        );
    }
}
