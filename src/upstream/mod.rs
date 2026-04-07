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

//! Upstream management: holds configured upstreams indexed by chain and provides
//! access to them for the RPC layer.

mod ethereum;
pub mod traits;

use crate::config::upstreams::{UpstreamConnection, UpstreamsConfig};
use ethereum::http::EthereumHttpUpstream;
use ethereum::ws::EthereumWsUpstream;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use traits::RpcUpstream;

/// Holds all configured upstreams, indexed by chain ID.
pub struct UpstreamManager {
    /// Map from `ChainRef` integer value to the upstream for that chain.
    upstreams: HashMap<i32, Arc<dyn RpcUpstream>>,
}

impl UpstreamManager {
    /// Build upstreams from the parsed configuration.
    ///
    /// For Ethereum connections: prefers WebSocket when available, falls back to
    /// HTTP. Bitcoin and Dshackle gRPC connections are not yet supported.
    pub fn from_config(config: &UpstreamsConfig) -> anyhow::Result<Self> {
        let mut upstreams: HashMap<i32, Arc<dyn RpcUpstream>> = HashMap::new();

        for upstream in &config.upstreams {
            if !upstream.enabled {
                tracing::debug!("Upstream {} is disabled, skipping", upstream.id);
                continue;
            }

            let blockchain_name = match &upstream.blockchain {
                Some(name) => name,
                None => {
                    tracing::warn!("Upstream {} has no blockchain specified, skipping", upstream.id);
                    continue;
                }
            };

            let chain = match emerald_api::proto::common::ChainRef::from_str(blockchain_name) {
                Ok(c) => c,
                Err(_) => {
                    tracing::warn!("Unknown blockchain '{}' for upstream {}, skipping", blockchain_name, upstream.id);
                    continue;
                }
            };

            match &upstream.connection {
                UpstreamConnection::Ethereum(eth) => {
                    let reader: Arc<dyn RpcUpstream> = if let Some(ws) = &eth.ws {
                        // Prefer WebSocket when configured
                        if ws.basic_auth.is_some() {
                            tracing::warn!("Upstream {}: WS basic auth not yet supported, ignoring", upstream.id);
                        }
                        tracing::info!(
                            "Registered Ethereum WS upstream '{}' at {} for {}",
                            upstream.id, ws.url, blockchain_name,
                        );
                        Arc::new(EthereumWsUpstream::new(
                            upstream.id.clone(),
                            ws.url.clone(),
                        ))
                    } else if let Some(rpc) = &eth.rpc {
                        if rpc.basic_auth.is_some() {
                            tracing::warn!("Upstream {}: basic auth not yet supported, ignoring", upstream.id);
                        }
                        if rpc.tls.is_some() {
                            tracing::warn!("Upstream {}: client TLS not yet supported, ignoring", upstream.id);
                        }
                        tracing::info!(
                            "Registered Ethereum HTTP upstream '{}' at {} for {}",
                            upstream.id, rpc.url, blockchain_name,
                        );
                        Arc::new(EthereumHttpUpstream::new(
                            upstream.id.clone(),
                            rpc.url.clone(),
                        ))
                    } else {
                        tracing::warn!("Upstream {} has no RPC or WS configured, skipping", upstream.id);
                        continue;
                    };

                    upstreams.insert(chain as i32, reader);
                }
                UpstreamConnection::Bitcoin(_) => {
                    tracing::warn!("Upstream {}: Bitcoin not yet supported, skipping", upstream.id);
                }
                UpstreamConnection::Dshackle(_) => {
                    tracing::warn!("Upstream {}: Dshackle gRPC not yet supported, skipping", upstream.id);
                }
            }
        }

        if upstreams.is_empty() {
            tracing::warn!("No usable upstreams were configured");
        }

        Ok(UpstreamManager { upstreams })
    }

    /// Look up the upstream for a given chain (identified by its protobuf i32 value).
    pub fn get(&self, chain: i32) -> Option<&Arc<dyn RpcUpstream>> {
        self.upstreams.get(&chain)
    }
}
