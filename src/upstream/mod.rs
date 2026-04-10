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
mod multistream;
mod switch;
pub mod traits;

use crate::blockchain::TargetBlockchain;
use crate::config::upstreams::{UpstreamConnection, UpstreamsConfig};
use ethereum::http::EthereumHttpUpstream;
use ethereum::ws::EthereumWsUpstream;
use multistream::Multistream;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use switch::SwitchClient;
use traits::RpcUpstream;

/// Holds all configured upstreams, indexed by target blockchain.
pub struct UpstreamManager {
    upstreams: HashMap<TargetBlockchain, Arc<dyn RpcUpstream>>,
}

impl UpstreamManager {
    /// Build upstreams from the parsed configuration.
    ///
    /// For Ethereum connections with both WS and HTTP configured, wraps them in
    /// a `SwitchClient` (WS primary, HTTP secondary). Otherwise uses whichever
    /// is available. Bitcoin and Dshackle gRPC connections are not yet supported.
    pub fn from_config(config: &UpstreamsConfig) -> anyhow::Result<Self> {
        let mut per_chain: HashMap<TargetBlockchain, Vec<Arc<dyn RpcUpstream>>> = HashMap::new();

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

            let chain: TargetBlockchain = match blockchain_name.parse() {
                Ok(c) => c,
                Err(_) => {
                    tracing::warn!("Unknown blockchain '{}' for upstream {}, skipping", blockchain_name, upstream.id);
                    continue;
                }
            };

            match &upstream.connection {
                UpstreamConnection::Ethereum(eth) => {
                    let ws_upstream: Option<Arc<dyn RpcUpstream>> = eth.ws.as_ref().map(|ws| {
                        if ws.basic_auth.is_some() {
                            tracing::warn!("Upstream {}: WS basic auth not yet supported, ignoring", upstream.id);
                        }
                        tracing::info!(
                            "Using Ethereum WS upstream '{}' at {} for {}",
                            upstream.id, ws.url, blockchain_name,
                        );
                        Arc::new(EthereumWsUpstream::new(
                            upstream.id.clone(),
                            ws.url.clone(),
                        )) as Arc<dyn RpcUpstream>
                    });

                    let http_upstream: Option<Arc<dyn RpcUpstream>> = eth.rpc.as_ref().map(|rpc| {
                        if rpc.basic_auth.is_some() {
                            tracing::warn!("Upstream {}: basic auth not yet supported, ignoring", upstream.id);
                        }
                        if rpc.tls.is_some() {
                            tracing::warn!("Upstream {}: client TLS not yet supported, ignoring", upstream.id);
                        }
                        tracing::info!(
                            "Using Ethereum HTTP upstream '{}' at {} for {}",
                            upstream.id, rpc.url, blockchain_name,
                        );
                        Arc::new(EthereumHttpUpstream::new(
                            upstream.id.clone(),
                            rpc.url.clone(),
                        )) as Arc<dyn RpcUpstream>
                    });

                    let reader: Arc<dyn RpcUpstream> = match (ws_upstream, http_upstream) {
                        (Some(ws), Some(http)) => {
                            // WS primary, HTTP fallback
                            tracing::info!(
                                "Upstream '{}': using WS with HTTP fallback for {}",
                                upstream.id, blockchain_name,
                            );
                            Arc::new(SwitchClient::new(ws, http))
                        }
                        (Some(ws), None) => ws,
                        (None, Some(http)) => http,
                        (None, None) => {
                            tracing::warn!("Upstream {} has no RPC or WS configured, skipping", upstream.id);
                            continue;
                        }
                    };

                    per_chain.entry(chain).or_default().push(reader);
                }
                UpstreamConnection::Bitcoin(_) => {
                    tracing::warn!("Upstream {}: Bitcoin not yet supported, skipping", upstream.id);
                }
                UpstreamConnection::Dshackle(_) => {
                    tracing::warn!("Upstream {}: Dshackle gRPC not yet supported, skipping", upstream.id);
                }
            }
        }

        let upstreams: HashMap<TargetBlockchain, Arc<dyn RpcUpstream>> = per_chain
            .into_iter()
            .map(|(chain, mut readers)| {
                let upstream: Arc<dyn RpcUpstream> = if readers.len() == 1 {
                    readers.remove(0)
                } else {
                    tracing::info!(
                        "{}: aggregating {} upstreams with round-robin",
                        chain, readers.len(),
                    );
                    Arc::new(Multistream::new(readers))
                };
                (chain, upstream)
            })
            .collect();

        if upstreams.is_empty() {
            tracing::warn!("No usable upstreams were configured");
        }

        Ok(UpstreamManager { upstreams })
    }

    /// Look up the upstream for a given blockchain.
    pub fn get(&self, chain: &TargetBlockchain) -> Option<&Arc<dyn RpcUpstream>> {
        self.upstreams.get(chain)
    }
}
