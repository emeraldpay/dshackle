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

pub mod availability;
pub(crate) mod bitcoin;
mod dshackle;
pub mod egress;
pub mod ethereum;
pub mod fork;
pub mod head;
pub mod merged_head;
mod methods;
pub mod multistream;
pub mod quorum;
pub mod router;
pub mod state;
mod status;
mod switch;
pub mod traits;
pub mod validation;

pub use multistream::Multistream;

use crate::blockchain::{BlockchainType, TargetBlockchain};
use crate::cache::{
    BitcoinCacheCodec, Caches, CachingHead, CachingUpstream, EthereumCacheCodec,
    EthereumNormalizer, NormalizingUpstream, RedisCache, redis_cache,
};
use crate::config::cache::CacheConfig;
use crate::config::upstreams::{UpstreamConnection, UpstreamsConfig};
use bitcoin::head::start_head_poller as start_btc_head_poller;
use bitcoin::http::BitcoinHttpUpstream;
use bitcoin::validator::BitcoinValidator;
use dshackle::DshackleUpstream;
use dshackle::head::start_head_subscriber;
use emerald_api::proto::blockchain::DescribeRequest;
use emerald_api::proto::blockchain::blockchain_client::BlockchainClient;
use emerald_api::proto::common::ChainRef;
use ethereum::EthereumWsUpstream;
use ethereum::head::{start_head_poller, start_ws_head};
use ethereum::http::EthereumHttpUpstream;
use ethereum::validator::EthereumValidator;
use fork::{
    DifficultyForkChoice, ForkChoice, ForkMember, PriorityForkChoice, is_pos, start_fork_watch,
};
use egress::{EgressSubscription, EthereumEgress, SyncingStatus};
use head::CurrentHead;
use merged_head::MergedHead;
use methods::AggregatedMethods;
use methods::ConfiguredMethods;
use methods::DefaultMethods;
use methods::HardcodedMethods;
use methods::LayeredMethods;
use methods::MethodFilter;
use methods::bitcoin::DefaultBitcoinMethods;
use methods::ethereum::DefaultEthereumMethods;
use quorum::QuorumFactory;
use status::ChainStatus;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use switch::SwitchClient;
use traits::RpcUpstream;

/// Holds all configured upstreams, indexed by target blockchain.
///
/// Each chain entry is a [`Multistream`] aggregate — even chains with a single
/// configured upstream are wrapped uniformly so the call path is the same.
pub struct UpstreamManager {
    upstreams: HashMap<TargetBlockchain, Arc<Multistream>>,
    caches: HashMap<TargetBlockchain, Arc<Caches>>,
    /// Per-chain merged head stream, used by `SubscribeHead` and the proxy's
    /// `newHeads` subscription.
    heads: HashMap<TargetBlockchain, Arc<MergedHead>>,
}

impl UpstreamManager {
    /// Build upstreams from the parsed configuration.
    ///
    /// For Ethereum connections with both WS and HTTP configured, wraps them in
    /// a `SwitchClient` (WS primary, HTTP secondary). Otherwise uses whichever
    /// is available. Dshackle gRPC upstreams discover chains dynamically via
    /// the remote's `Describe` RPC.
    pub async fn from_config(
        config: &UpstreamsConfig,
        cache_config: Option<&CacheConfig>,
    ) -> anyhow::Result<Self> {
        // A configured but unreachable Redis aborts the startup (matching
        // the legacy behavior): silently running without the expected shared
        // cache would overload the upstreams.
        let redis = match cache_config.and_then(|c| c.redis()) {
            Some(redis_config) => Some(redis_cache::connect(redis_config).await?),
            None => None,
        };

        let mut per_chain: HashMap<TargetBlockchain, Vec<Arc<dyn RpcUpstream>>> = HashMap::new();
        // Per-upstream method configs, collected alongside the readers so the
        // chain-level factory can aggregate them. Order matches `per_chain`
        // so "first delegate supporting the method" preserves config order.
        let mut per_chain_methods: HashMap<TargetBlockchain, Vec<Arc<dyn QuorumFactory>>> =
            HashMap::new();
        // Per-upstream fork-watch members, collected so the per-chain fork
        // choice can be built once all upstreams of a chain are known.
        let mut per_chain_fork: HashMap<TargetBlockchain, Vec<ForkMember>> = HashMap::new();
        // Per-upstream head trackers, collected so a per-chain merged head can
        // be built once all upstreams of a chain are known.
        let mut per_chain_heads: HashMap<TargetBlockchain, Vec<Arc<CurrentHead>>> = HashMap::new();
        // Per-chain caches and caching heads. Created lazily on first upstream
        // for each chain. The CachingHead subscribes to each upstream's block
        // stream and deduplicates before writing to the cache — one update per
        // chain regardless of how many upstreams report the same block.
        let mut per_chain_caches: HashMap<TargetBlockchain, Arc<Caches>> = HashMap::new();
        let mut per_chain_caching_heads: HashMap<TargetBlockchain, CachingHead> = HashMap::new();

        for upstream in &config.upstreams {
            if !upstream.enabled {
                tracing::debug!("Upstream {} is disabled, skipping", upstream.id);
                continue;
            }

            match &upstream.connection {
                UpstreamConnection::Ethereum(eth) => {
                    let chain = match parse_required_chain(upstream) {
                        Some(c) => c,
                        None => continue,
                    };
                    let blockchain_name = upstream.blockchain.as_deref().unwrap_or("?");

                    // Build WS and HTTP transports. Head tracking is started
                    // on only one of them — WS is preferred because newHeads
                    // gives near-instant updates, while HTTP polls every 10s.
                    // The other transport is used for RPC calls only.
                    let ws_upstream: Option<Arc<EthereumWsUpstream>> = eth.ws.as_ref().map(|ws| {
                        if ws.basic_auth.is_some() {
                            tracing::warn!(
                                "Upstream {}: WS basic auth not yet supported, ignoring",
                                upstream.id
                            );
                        }
                        tracing::info!(
                            "Using Ethereum WS upstream '{}' at {} for {}",
                            upstream.id,
                            ws.url,
                            blockchain_name,
                        );
                        Arc::new(EthereumWsUpstream::new(
                            upstream.id.clone(),
                            ws.url.clone(),
                            ws.connections.unwrap_or(1),
                        ))
                    });

                    let http_upstream: Option<Arc<EthereumHttpUpstream>> =
                        eth.rpc.as_ref().map(|rpc| {
                            if rpc.basic_auth.is_some() {
                                tracing::warn!(
                                    "Upstream {}: basic auth not yet supported, ignoring",
                                    upstream.id
                                );
                            }
                            if rpc.tls.is_some() {
                                tracing::warn!(
                                    "Upstream {}: client TLS not yet supported, ignoring",
                                    upstream.id
                                );
                            }
                            tracing::info!(
                                "Using Ethereum HTTP upstream '{}' at {} for {}",
                                upstream.id,
                                rpc.url,
                                blockchain_name,
                            );
                            Arc::new(EthereumHttpUpstream::new(
                                upstream.id.clone(),
                                rpc.url.clone(),
                            ))
                        });

                    // Start head tracking on exactly one transport per
                    // upstream and subscribe the per-chain CachingHead to it.
                    let caching_head = get_or_create_caching_head(
                        chain,
                        redis.as_ref(),
                        &mut per_chain_caches,
                        &mut per_chain_caching_heads,
                    );
                    match (&ws_upstream, &http_upstream) {
                        (Some(ws_up), _) => {
                            caching_head.follow(&ws_up.head_height());
                            start_ws_head(Arc::clone(ws_up));
                        }
                        (None, Some(http_up)) => {
                            let head = http_up.head_height();
                            caching_head.follow(&head);
                            start_head_poller(
                                upstream.id.clone(),
                                Arc::clone(http_up) as Arc<dyn RpcUpstream>,
                                head,
                            );
                        }
                        (None, None) => {
                            // handled below
                        }
                    }

                    // The head being tracked (WS primary, else HTTP) is the one
                    // the fork watcher follows for this upstream.
                    let fork_head: Option<Arc<CurrentHead>> = ws_upstream
                        .as_ref()
                        .map(|u| u.head_height())
                        .or_else(|| http_upstream.as_ref().map(|u| u.head_height()));

                    let ws_rpc: Option<Arc<dyn RpcUpstream>> =
                        ws_upstream.map(|u| u as Arc<dyn RpcUpstream>);
                    let http_rpc: Option<Arc<dyn RpcUpstream>> =
                        http_upstream.map(|u| u as Arc<dyn RpcUpstream>);

                    let reader: Arc<dyn RpcUpstream> = match (ws_rpc, http_rpc) {
                        (Some(ws), Some(http)) => {
                            // WS primary, HTTP fallback
                            tracing::info!(
                                "Upstream '{}': using WS with HTTP fallback for {}",
                                upstream.id,
                                blockchain_name,
                            );
                            Arc::new(SwitchClient::new(ws, http))
                        }
                        (Some(ws), None) => ws,
                        (None, Some(http)) => http,
                        (None, None) => {
                            tracing::warn!(
                                "Upstream {} has no RPC or WS configured, skipping",
                                upstream.id
                            );
                            continue;
                        }
                    };

                    // Validation probes the transport directly (`reader` at
                    // this point), below the method-filter and cache wrappers
                    // added later: a user's method allow-list must not fail
                    // the probes, and a cached answer must not pass them.
                    let options = config.options_for(upstream);
                    if options.disable_validation {
                        tracing::warn!("Disable validation for upstream {}", upstream.id);
                        reader.state().set_always_valid();
                    } else {
                        validation::start_validation(
                            Arc::clone(&reader),
                            Box::new(EthereumValidator::new(chain, options.clone())),
                            options.validation_interval,
                        );
                    }

                    // Compose the chain-default layer with the user's
                    // configured overrides via `LayeredMethods`. The same
                    // instance feeds the per-upstream wrappers and the
                    // chain-level `AggregatedMethods` factory.
                    let default_layer: Arc<dyn QuorumFactory> =
                        Arc::new(DefaultEthereumMethods::new(chain));
                    let configured_layer =
                        Arc::new(ConfiguredMethods::from_config(upstream.methods.as_ref()));
                    let methods: Arc<dyn QuorumFactory> =
                        Arc::new(LayeredMethods::new(default_layer, configured_layer));

                    // Wrapping order (outermost first):
                    //   HardcodedMethods → NormalizingUpstream → CachingUpstream
                    //     → MethodFilter → transport
                    // Hardcoded responses are cheapest, then cache, then network.
                    // Normalizing sits above the cache so that a request
                    // rewritten to block-by-hash can be served from the cache.
                    let caches = per_chain_caches
                        .get(&chain)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(Caches::new()));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(MethodFilter::new(reader, Arc::clone(&methods)));
                    let reader: Arc<dyn RpcUpstream> = Arc::new(CachingUpstream::new(
                        reader,
                        Arc::clone(&caches),
                        EthereumCacheCodec,
                    ));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(NormalizingUpstream::new(reader, caches, EthereumNormalizer));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(HardcodedMethods::new(reader, Arc::clone(&methods)));

                    if let Some(head) = fork_head {
                        per_chain_heads
                            .entry(chain)
                            .or_default()
                            .push(Arc::clone(&head));
                        per_chain_fork.entry(chain).or_default().push(ForkMember {
                            id: upstream.id.clone(),
                            priority: options.priority,
                            head,
                            state: Arc::clone(reader.state()),
                        });
                    }

                    per_chain.entry(chain).or_default().push(reader);
                    per_chain_methods.entry(chain).or_default().push(methods);
                }
                UpstreamConnection::Bitcoin(btc) => {
                    let chain = match parse_required_chain(upstream) {
                        Some(c) => c,
                        None => continue,
                    };
                    let blockchain_name = upstream.blockchain.as_deref().unwrap_or("?");

                    let rpc = match &btc.rpc {
                        Some(rpc) => rpc,
                        None => {
                            tracing::warn!(
                                "Upstream {} has no RPC configured, skipping",
                                upstream.id
                            );
                            continue;
                        }
                    };

                    if rpc.tls.is_some() {
                        tracing::warn!(
                            "Upstream {}: client TLS not yet supported, ignoring",
                            upstream.id
                        );
                    }

                    let basic_auth = rpc
                        .basic_auth
                        .as_ref()
                        .map(|auth| (auth.username.clone(), auth.password.clone()));

                    tracing::info!(
                        "Using Bitcoin HTTP upstream '{}' at {} for {}",
                        upstream.id,
                        rpc.url,
                        blockchain_name,
                    );

                    let http_up =
                        BitcoinHttpUpstream::new(upstream.id.clone(), rpc.url.clone(), basic_auth);
                    let head = http_up.head_height();
                    let caching_head = get_or_create_caching_head(
                        chain,
                        redis.as_ref(),
                        &mut per_chain_caches,
                        &mut per_chain_caching_heads,
                    );
                    caching_head.follow(&head);
                    let reader: Arc<dyn RpcUpstream> = Arc::new(http_up);
                    let fork_head = Arc::clone(&head);
                    start_btc_head_poller(upstream.id.clone(), Arc::clone(&reader), head);

                    // See the Ethereum branch for why validation targets the
                    // bare transport.
                    let options = config.options_for(upstream);
                    if options.disable_validation {
                        tracing::warn!("Disable validation for upstream {}", upstream.id);
                        reader.state().set_always_valid();
                    } else {
                        validation::start_validation(
                            Arc::clone(&reader),
                            Box::new(BitcoinValidator::new(options.clone())),
                            options.validation_interval,
                        );
                    }

                    // See the Ethereum branch for why the same instance feeds
                    // the wrappers and the aggregator.
                    let default_layer: Arc<dyn QuorumFactory> =
                        Arc::new(DefaultBitcoinMethods::new());
                    let configured_layer =
                        Arc::new(ConfiguredMethods::from_config(upstream.methods.as_ref()));
                    let methods: Arc<dyn QuorumFactory> =
                        Arc::new(LayeredMethods::new(default_layer, configured_layer));

                    let caches = per_chain_caches
                        .get(&chain)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(Caches::new()));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(MethodFilter::new(reader, Arc::clone(&methods)));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(CachingUpstream::new(reader, caches, BitcoinCacheCodec));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(HardcodedMethods::new(reader, Arc::clone(&methods)));

                    per_chain_heads
                        .entry(chain)
                        .or_default()
                        .push(Arc::clone(&fork_head));
                    per_chain_fork.entry(chain).or_default().push(ForkMember {
                        id: upstream.id.clone(),
                        priority: options.priority,
                        head: fork_head,
                        state: Arc::clone(reader.state()),
                    });

                    per_chain.entry(chain).or_default().push(reader);
                    per_chain_methods.entry(chain).or_default().push(methods);
                }
                UpstreamConnection::Dshackle(ds) => {
                    let url = match resolve_dshackle_url(ds) {
                        Some(u) => u,
                        None => {
                            tracing::warn!(
                                "Upstream {}: no URL or host configured for Dshackle connection, skipping",
                                upstream.id
                            );
                            continue;
                        }
                    };

                    if ds.tls.is_some() {
                        tracing::warn!(
                            "Upstream {}: Dshackle TLS not yet supported, ignoring",
                            upstream.id
                        );
                    }

                    tracing::info!("Connecting to remote Dshackle '{}' at {}", upstream.id, url,);

                    match connect_dshackle(
                        &upstream.id,
                        &url,
                        redis.as_ref(),
                        &mut per_chain_caches,
                        &mut per_chain_caching_heads,
                    )
                    .await
                    {
                        Ok(discovered) => {
                            for (chain, reader) in discovered {
                                per_chain.entry(chain).or_default().push(reader);
                                // Remote Dshackles handle their own quorum
                                // internally; use `DefaultMethods` so the
                                // aggregator treats their methods as callable.
                                per_chain_methods
                                    .entry(chain)
                                    .or_default()
                                    .push(Arc::new(DefaultMethods));
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Upstream {}: failed to connect to Dshackle at {}: {}",
                                upstream.id,
                                url,
                                e
                            );
                        }
                    }
                }
            }
        }

        // Start fork detection per chain. PoS chains compare by priority;
        // everything else falls back to cumulative-difficulty ordering. The
        // fork choice is shared across the chain's upstreams so it sees them
        // all; each upstream then gets its own watcher.
        for (chain, members) in per_chain_fork {
            let TargetBlockchain::Standard(chain_ref) = chain;
            let fork_choice: Arc<dyn ForkChoice> = if is_pos(chain_ref) {
                let priority = Arc::new(PriorityForkChoice::new());
                for member in &members {
                    priority.add_upstream(
                        member.id.clone(),
                        member.priority,
                        Arc::clone(&member.state),
                    );
                }
                priority
            } else {
                Arc::new(DifficultyForkChoice::new())
            };
            for member in members {
                start_fork_watch(
                    member.id,
                    member.head,
                    member.state,
                    Arc::clone(&fork_choice),
                );
            }
        }

        let mut upstreams: HashMap<TargetBlockchain, Arc<Multistream>> = HashMap::new();
        let mut chain_statuses: Vec<ChainStatus> = Vec::new();

        for (chain, readers) in per_chain {
            let chain_status = ChainStatus {
                chain,
                upstreams: readers.clone(),
            };
            chain_statuses.push(chain_status);

            if readers.len() > 1 {
                tracing::info!("{}: aggregating {} upstreams", chain, readers.len(),);
            }
            let delegates = per_chain_methods.remove(&chain).unwrap_or_default();
            let factory: Arc<dyn QuorumFactory> = if delegates.is_empty() {
                quorum_factory_for(chain)
            } else {
                Arc::new(AggregatedMethods::new(delegates))
            };
            upstreams.insert(chain, Arc::new(Multistream::new(readers, factory)));
        }

        if upstreams.is_empty() {
            tracing::warn!("No usable upstreams were configured");
        }

        // Merge each chain's upstream heads into a single best-head stream for
        // `SubscribeHead` (remote Dshackle heads are folded in with section 4).
        let heads = per_chain_heads
            .into_iter()
            .map(|(chain, chain_heads)| (chain, MergedHead::new(chain_heads)))
            .collect();

        status::start_status_reporter(chain_statuses);

        Ok(UpstreamManager {
            upstreams,
            caches: per_chain_caches,
            heads,
        })
    }

    /// Direct constructor for callers that already hold the resolved
    /// per-chain `Multistream`s and `Caches` — primarily code that wires
    /// upstreams outside the standard config-driven path (e.g. tests).
    pub(crate) fn from_parts(
        upstreams: HashMap<TargetBlockchain, Arc<Multistream>>,
        caches: HashMap<TargetBlockchain, Arc<Caches>>,
    ) -> Self {
        Self {
            upstreams,
            caches,
            heads: HashMap::new(),
        }
    }

    /// Look up the upstream aggregate for a given blockchain.
    pub fn get(&self, chain: &TargetBlockchain) -> Option<&Arc<Multistream>> {
        self.upstreams.get(chain)
    }

    /// Look up the merged head stream for a given blockchain.
    pub fn head(&self, chain: &TargetBlockchain) -> Option<&Arc<MergedHead>> {
        self.heads.get(chain)
    }

    /// Build the `eth_subscribe` egress for a chain, or `None` when the chain
    /// can't serve server-pushed subscriptions: it tracks no head yet, or it
    /// isn't an Ethereum-family chain. Bitcoin egress isn't ported, so Bitcoin
    /// chains reject `eth_subscribe` rather than emitting Ethereum-shaped
    /// notifications built from Bitcoin blocks.
    pub fn egress(&self, chain: &TargetBlockchain) -> Option<Arc<dyn EgressSubscription>> {
        if chain.blockchain_type() != BlockchainType::Ethereum {
            return None;
        }
        let head = self.head(chain)?;
        let status: Arc<dyn SyncingStatus> = self.get(chain)?.clone();
        Some(Arc::new(EthereumEgress::new(Arc::clone(head), status)))
    }

    /// Look up the cache for a given blockchain.
    pub fn caches(&self, chain: &TargetBlockchain) -> Option<&Arc<Caches>> {
        self.caches.get(chain)
    }
}

// ─── Helpers ───────────────────────────────────────────────────────────────

/// Returns a reference to the `CachingHead` for a chain, creating it (and the
/// underlying `Caches`) on first access.
fn get_or_create_caching_head<'a>(
    chain: TargetBlockchain,
    redis_conn: Option<&redis::aio::ConnectionManager>,
    caches_map: &mut HashMap<TargetBlockchain, Arc<Caches>>,
    caching_heads: &'a mut HashMap<TargetBlockchain, CachingHead>,
) -> &'a CachingHead {
    let caches = caches_map.entry(chain).or_insert_with(|| {
        let redis = redis_conn.map(|conn| RedisCache::new(conn.clone(), chain.id()));
        Arc::new(Caches::with_redis(redis))
    });
    caching_heads
        .entry(chain)
        .or_insert_with(|| CachingHead::new(Arc::clone(caches)))
}

/// Parses the blockchain field from a configured upstream. Returns `None` and
/// logs a warning if the field is missing or unrecognized.
fn parse_required_chain(upstream: &crate::config::upstreams::Upstream) -> Option<TargetBlockchain> {
    let name = match &upstream.blockchain {
        Some(name) => name,
        None => {
            tracing::warn!(
                "Upstream {} has no blockchain specified, skipping",
                upstream.id
            );
            return None;
        }
    };
    match name.parse() {
        Ok(c) => Some(c),
        Err(_) => {
            tracing::warn!(
                "Unknown blockchain '{}' for upstream {}, skipping",
                name,
                upstream.id
            );
            None
        }
    }
}

/// Resolves the gRPC URL for a Dshackle connection from its config.
fn resolve_dshackle_url(ds: &crate::config::upstreams::DshackleConnection) -> Option<String> {
    if let Some(url) = &ds.url {
        return Some(url.clone());
    }
    ds.host.as_ref().map(|host| {
        let port = ds.port.unwrap_or(2448);
        format!("http://{host}:{port}")
    })
}

/// Returns the appropriate syncing lag threshold for a chain.
fn syncing_lag_for(chain_ref: ChainRef) -> u64 {
    match chain_ref {
        ChainRef::ChainBitcoin | ChainRef::ChainTestnetBitcoin | ChainRef::ChainTestnetBitcoin4 => {
            2
        }
        _ => 6,
    }
}

/// Fallback chain-default factory for when no per-upstream method config was
/// collected (e.g. a chain whose only upstream is a Dshackle remote that
/// hit a connection error). Normal flow aggregates the per-upstream factories
/// collected by `from_config` instead.
fn quorum_factory_for(chain: TargetBlockchain) -> Arc<dyn QuorumFactory> {
    match chain.blockchain_type() {
        BlockchainType::Bitcoin => Arc::new(DefaultBitcoinMethods::new()),
        BlockchainType::Ethereum => Arc::new(DefaultEthereumMethods::new(chain)),
        BlockchainType::Unknown => Arc::new(DefaultMethods),
    }
}

/// Connects to a remote Dshackle instance, calls `Describe` to discover
/// available chains, and creates a per-chain upstream for each.
async fn connect_dshackle(
    upstream_id: &str,
    url: &str,
    redis_conn: Option<&redis::aio::ConnectionManager>,
    caches_map: &mut HashMap<TargetBlockchain, Arc<Caches>>,
    caching_heads: &mut HashMap<TargetBlockchain, CachingHead>,
) -> anyhow::Result<Vec<(TargetBlockchain, Arc<dyn RpcUpstream>)>> {
    let endpoint = tonic::transport::Endpoint::from_shared(url.to_string())?;
    let channel = endpoint.connect().await?;
    let mut client = BlockchainClient::new(channel);

    let describe_resp = client.describe(DescribeRequest {}).await?;
    let chains = describe_resp.into_inner().chains;

    if chains.is_empty() {
        tracing::warn!(
            "Dshackle '{}': remote reported no available chains",
            upstream_id
        );
        return Ok(Vec::new());
    }

    let mut results = Vec::new();

    for desc_chain in &chains {
        let chain_ref = match ChainRef::try_from(desc_chain.chain) {
            Ok(c) if c != ChainRef::ChainUnspecified => c,
            _ => {
                tracing::debug!(
                    "Dshackle '{}': skipping unknown chain id {}",
                    upstream_id,
                    desc_chain.chain
                );
                continue;
            }
        };

        let chain = TargetBlockchain::from(chain_ref);
        let chain_id = format!("{}/{}", upstream_id, chain);
        let syncing_lag = syncing_lag_for(chain_ref);

        tracing::info!(
            "Dshackle '{}': discovered chain {} ({} methods)",
            upstream_id,
            chain,
            desc_chain.supported_methods.len(),
        );

        let ds_upstream = DshackleUpstream::new(
            chain_id.clone(),
            desc_chain.chain,
            client.clone(),
            syncing_lag,
        );

        // Subscribe caching head before starting the poller so no blocks are missed
        let head = ds_upstream.head_height();
        let caching_head = get_or_create_caching_head(chain, redis_conn, caches_map, caching_heads);
        caching_head.follow(&head);

        // Start head tracking via SubscribeHead
        start_head_subscriber(
            chain_id.clone(),
            desc_chain.chain,
            ds_upstream.grpc_client(),
            head,
        );

        let reader: Arc<dyn RpcUpstream> = Arc::new(ds_upstream);

        // Use the supported methods from Describe as the allowed set.
        // The remote Dshackle already handles hardcoded responses, so we
        // only need a MethodFilter — no HardcodedMethods wrapper.
        if !desc_chain.supported_methods.is_empty() {
            let callable: HashSet<_> = desc_chain
                .supported_methods
                .iter()
                .filter_map(|m| m.parse().ok())
                .collect();
            let methods: Arc<dyn QuorumFactory> =
                Arc::new(ConfiguredMethods::allowed_only(callable));
            let reader = Arc::new(MethodFilter::new(reader, methods));
            results.push((chain, reader as Arc<dyn RpcUpstream>));
        } else {
            // No method list — pass everything through
            results.push((chain, reader));
        }
    }

    tracing::info!(
        "Dshackle '{}': connected with {} chain(s)",
        upstream_id,
        results.len(),
    );

    Ok(results)
}
