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

pub mod allowance;
pub mod availability;
pub mod balance;
pub(crate) mod bitcoin;
pub mod block_updates;
pub mod call_plan;
mod call_status;
pub(crate) mod chains;
mod disabled_methods;
mod dshackle;
pub mod egress;
pub mod ethereum;
pub mod fanout;
pub mod fees;
pub mod fork;
pub mod head;
pub mod http_error;
pub mod id;
mod identified;
pub mod label;
mod lag;
mod limits;
mod logged;
pub mod merged_head;
mod metered;
mod methods;
pub mod multistream;
mod overload;
mod pause;
pub mod quorum;
pub mod router;
pub mod selector;
pub mod state;
mod status;
pub mod status_signal;
mod switch;
pub mod traits;
pub mod tx_status;
pub mod validation;

pub use id::UpstreamId;
pub use identified::IdentifiedUpstream;
pub use label::UpstreamLabels;
pub use multistream::Multistream;

use crate::blockchain::{BlockchainType, TargetBlockchain};
use crate::cache::{BitcoinCacheCodec, Caches, CachingUpstream, EthereumCacheCodec, redis_cache};
use crate::config::cache::CacheConfig;
use crate::config::tokens::{TokenConfig, TokenType};
use crate::config::upstreams::{UpstreamConnection, UpstreamsConfig};
use allowance::{AllowanceError, AllowanceStream};
use anyhow::Context as _;
use balance::esplora_unspent::EsploraUnspentSource;
use balance::rpc_unspent::RpcUnspentSource;
use balance::unspent::UnspentSource;
use balance::{BalanceError, BalanceStream, BitcoinBalance, EthereumBalance};
use bitcoin::esplora::EsploraClient;
use bitcoin::head::start_head_poller as start_btc_head_poller;
use bitcoin::http::BitcoinHttpUpstream;
use bitcoin::reader::BitcoinReader;
use bitcoin::subscribe::{BitcoinEgress, BitcoinZmqTopic, ZmqTopicStream};
use bitcoin::validator::BitcoinValidator;
use bitcoin::zmq::start_zmq_head;
use call_plan::{AsIs, CallPlanner};
use chains::{ChainMember, Chains};
use dshackle::connection::{RemoteClient, RemoteEndpoint};
use dshackle::remote::RemoteDshackle;
use egress::{ChainAccess, EgressSubscription, EthereumEgress, MergedEgress, TopicSubset};
use emerald_api::proto::blockchain::AddressAllowanceRequest;
use emerald_api::proto::blockchain::BalanceRequest;
use emerald_api::proto::blockchain::TxStatusRequest;
use emerald_api::proto::common::ChainRef;
use ethereum::EthereumWsUpstream;
use ethereum::call_plan::EthereumCallPlanner;
use ethereum::head::{start_head_poller, start_ws_head};
use ethereum::http::EthereumHttpUpstream;
use ethereum::pending_tx::{PendingTxSource, WsPendingTxes};
use ethereum::validator::EthereumValidator;
use fees::{BitcoinFees, ChainFees, EthereumFees};
use fork::ForkMember;
use head::CurrentHead;
use limits::{RequestBudget, RequestLimitGuard};
use logged::LoggedUpstream;
use merged_head::MergedHead;
use metered::MeteredUpstream;
use methods::ConfiguredMethods;
use methods::HardcodedMethods;
use methods::LayeredMethods;
use methods::MethodFilter;
use methods::bitcoin::DefaultBitcoinMethods;
use methods::ethereum::DefaultEthereumMethods;
use overload::OverloadGuard;
use quorum::QuorumFactory;
use state::UpstreamState;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use switch::SwitchClient;
use traits::{Capability, RpcUpstream};
use tx_status::bitcoin::BitcoinTxReader;
use tx_status::ethereum::EthereumTxReader;
use tx_status::{TxStatusError, TxStatusStream};

/// `User-Agent` sent on every connection to an upstream (HTTP, WS, gRPC).
pub(crate) const USER_AGENT: &str = concat!("EmeraldDshackle/", env!("CARGO_PKG_VERSION"));

/// Holds all configured upstreams, indexed by target blockchain.
///
/// Each chain entry is a [`Multistream`] aggregate — even chains with a single
/// configured upstream are wrapped uniformly so the call path is the same.
pub struct UpstreamManager {
    /// Everything per chain that a remote Dshackle connecting after the start
    /// may add to.
    chains: Arc<Chains>,
    /// Configured ERC-20 tokens, keyed by `(chain, lowercased name)` → the
    /// lowercased contract address. Lets a `GetBalance` request name a token by
    /// its configured code (legacy `TrackERC20Address` `tokens` map).
    tokens: HashMap<(TargetBlockchain, String), String>,
    /// Per-chain Esplora index configured on a Bitcoin upstream. When present
    /// it is the chain's source of unspent outputs, ahead of a remote Dshackle
    /// and of the node's own `listunspent` (legacy `CurrentUnspentReader`).
    esplora: HashMap<TargetBlockchain, Arc<EsploraClient>>,
    /// Per-chain ZMQ topic streams of the Bitcoin upstreams, backing the
    /// Bitcoin egress subscriptions.
    zmq_topics: HashMap<TargetBlockchain, Vec<Arc<ZmqTopicStream>>>,
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
        tokens: &[TokenConfig],
        config_dir: &std::path::Path,
    ) -> anyhow::Result<Self> {
        // A configured but unreachable Redis aborts the startup (matching
        // the legacy behavior): silently running without the expected shared
        // cache would overload the upstreams.
        let redis = match cache_config.and_then(|c| c.redis()) {
            Some(redis_config) => Some(redis_cache::connect(redis_config).await?),
            None => None,
        };

        let chains = Arc::new(Chains::new(redis));
        // Per-chain ZMQ topic streams of the Bitcoin upstreams, feeding the
        // chain's egress subscriptions (`NativeSubscribe`).
        let mut per_chain_zmq_topics: HashMap<TargetBlockchain, Vec<Arc<ZmqTopicStream>>> =
            HashMap::new();
        // First Esplora index configured per Bitcoin chain (legacy takes the
        // first upstream that has one).
        let mut per_chain_esplora: HashMap<TargetBlockchain, Arc<EsploraClient>> = HashMap::new();
        // Remote Dshackle upstreams to connect after the main loop. Their chains
        // and methods are only known from the remote's `Describe`, so they are
        // discovered concurrently instead of blocking startup one-by-one.
        let mut remotes: Vec<Arc<RemoteDshackle>> = Vec::new();

        for upstream in &config.upstreams {
            if !upstream.enabled {
                tracing::debug!("Upstream {} is disabled, skipping", upstream.id);
                continue;
            }
            // The config carries the id and labels as written; only an
            // upstream that is actually wired must have valid ones.
            let id = upstream.upstream_id()?;

            match &upstream.connection {
                UpstreamConnection::Ethereum(eth) => {
                    let labels = local_labels(upstream)?;
                    let chain = match parse_required_chain(upstream) {
                        Some(c) => c,
                        None => continue,
                    };
                    let blockchain_name = upstream.blockchain.as_deref().unwrap_or("?");
                    // Resolved before the transports: `options.timeout` bounds
                    // every call the clients make.
                    let options = config.options_for(upstream);

                    // Build WS and HTTP transports. Head tracking is started
                    // on only one of them — WS is preferred because newHeads
                    // gives near-instant updates, while HTTP polls every 10s.
                    // The other transport is used for RPC calls only.
                    // Shared by the WS transport and the WS/HTTP switch, so
                    // a WS handshake refused with 429 pauses the upstream as
                    // a whole: HTTP on the same provider key is refused too.
                    let upstream_state = Arc::new(UpstreamState::new());
                    let ws_upstream: Option<Arc<EthereumWsUpstream>> = eth.ws.as_ref().map(|ws| {
                        // The WS stack (tungstenite) implements no
                        // permessage-deflate, and advertising the extension
                        // without implementing it would corrupt the framing —
                        // so the option gets a loud warning instead of a
                        // silent ignore.
                        if ws.compress == Some(true) {
                            tracing::warn!(
                                "Upstream {}: WS compression is not supported and the 'compress' option is ignored",
                                id
                            );
                        }
                        tracing::info!(
                            "Using Ethereum WS upstream '{}' at {} for {}",
                            id,
                            ws.url,
                            blockchain_name,
                        );
                        Arc::new(EthereumWsUpstream::new(
                            id.clone(),
                            ethereum::WsTarget {
                                url: ws.url.clone(),
                                origin: ws.origin.clone(),
                                basic_auth: ws.basic_auth.clone(),
                                call_timeout: options.timeout,
                                frame_size: ws.frame_size,
                                msg_size: ws.msg_size,
                            },
                            ws.connections.unwrap_or(1),
                            Arc::clone(&upstream_state),
                        ))
                    });

                    let http_upstream: Option<Arc<EthereumHttpUpstream>> = match &eth.rpc {
                        Some(rpc) => {
                            let tls =
                                crate::tls::client_tls(id.as_str(), rpc.tls.as_ref(), config_dir)?;
                            let client = crate::tls::reqwest_client(
                                tls.as_ref(),
                                options.timeout,
                                rpc.compress.unwrap_or(false),
                            )?;
                            tracing::info!(
                                "Using Ethereum HTTP upstream '{}' at {} for {}",
                                id,
                                rpc.url,
                                blockchain_name,
                            );
                            Some(Arc::new(EthereumHttpUpstream::new(
                                id.clone(),
                                rpc.url.clone(),
                                rpc.basic_auth.clone(),
                                client,
                            )))
                        }
                        None => None,
                    };

                    // Start head tracking on exactly one transport per
                    // upstream and subscribe the per-chain CachingHead to it.
                    // That head is also the one the fork watcher follows for
                    // this upstream.
                    // `disabled-methods` keeps a method off the WS
                    // connection, and for `eth_subscribe` that leaves nothing
                    // to subscribe to: neither the head nor the pending
                    // transactions (legacy applied the list inside the
                    // connection, to its own subscriptions as well).
                    let ws_subscriptions_disabled = eth
                        .ws
                        .as_ref()
                        .is_some_and(|ws| ws.subscriptions_disabled());
                    if ws_subscriptions_disabled {
                        tracing::info!(
                            "Upstream {}: eth_subscribe is disabled for WS, so the head is polled and no pending transactions come from this upstream",
                            id
                        );
                    }
                    let fork_head: Option<Arc<CurrentHead>> = match (&ws_upstream, &http_upstream) {
                        (Some(ws_up), _) if !ws_subscriptions_disabled => {
                            let head = ws_up.head_height();
                            chains.cache_blocks_of(chain, &head);
                            start_ws_head(Arc::clone(ws_up));
                            chains.add_pending_source(
                                chain,
                                Arc::new(WsPendingTxes::new(Arc::clone(ws_up))),
                            );
                            Some(head)
                        }
                        (Some(ws_up), None) => {
                            // WS is the only transport, so the polling goes
                            // through it.
                            let head = ws_up.head_height();
                            chains.cache_blocks_of(chain, &head);
                            start_head_poller(
                                Arc::clone(ws_up) as Arc<dyn RpcUpstream>,
                                Arc::clone(&head),
                            );
                            Some(head)
                        }
                        (_, Some(http_up)) => {
                            let head = http_up.head_height();
                            chains.cache_blocks_of(chain, &head);
                            // The poller's requests go through an instrumented
                            // view of the transport: the legacy head poller
                            // shares the instrumented client, so its calls are
                            // counted and logged (as INTERNAL) too.
                            let metered = MeteredUpstream::new(
                                Arc::clone(http_up) as Arc<dyn RpcUpstream>,
                                crate::metrics::UpstreamProtocol::HttpJsonRpc,
                                id.clone(),
                                chain,
                            );
                            start_head_poller(
                                Arc::new(LoggedUpstream::new(
                                    Arc::new(metered),
                                    crate::logs::Channel::JsonRpc,
                                    id.clone(),
                                    chain,
                                )),
                                Arc::clone(&head),
                            );
                            Some(head)
                        }
                        (None, None) => {
                            // handled below
                            None
                        }
                    };

                    // Metering and request logging sit directly above each
                    // transport, so requests are attributed to the connection
                    // that carried them even when the WS/HTTP switch picks
                    // between the two.
                    let ws_rpc: Option<Arc<dyn RpcUpstream>> = ws_upstream.map(|u| {
                        let metered = MeteredUpstream::new(
                            u,
                            crate::metrics::UpstreamProtocol::WsJsonRpc,
                            id.clone(),
                            chain,
                        );
                        let logged: Arc<dyn RpcUpstream> = Arc::new(LoggedUpstream::new(
                            Arc::new(metered),
                            crate::logs::Channel::WsJsonRpc,
                            id.clone(),
                            chain,
                        ));
                        // The routing preference sits above the metering and
                        // logging stack, so a diverted call is neither counted
                        // nor logged as a WS request — it never was one
                        // (legacy short-circuits inside the connection, before
                        // its metrics, for the same reason).
                        match &eth.ws {
                            Some(ws) if !ws.disabled_methods.is_empty() => {
                                Arc::new(disabled_methods::DisabledMethods::new(
                                    logged,
                                    ws.disabled_methods.iter().map(|m| m.as_str().into()),
                                )) as Arc<dyn RpcUpstream>
                            }
                            _ => logged,
                        }
                    });
                    let http_rpc: Option<Arc<dyn RpcUpstream>> = http_upstream.map(|u| {
                        let metered = MeteredUpstream::new(
                            u,
                            crate::metrics::UpstreamProtocol::HttpJsonRpc,
                            id.clone(),
                            chain,
                        );
                        Arc::new(LoggedUpstream::new(
                            Arc::new(metered),
                            crate::logs::Channel::JsonRpc,
                            id.clone(),
                            chain,
                        )) as Arc<dyn RpcUpstream>
                    });

                    let reader: Arc<dyn RpcUpstream> = match (ws_rpc, http_rpc) {
                        (Some(ws), Some(http)) => {
                            // WS primary, HTTP fallback
                            tracing::info!(
                                "Upstream '{}': using WS with HTTP fallback for {}",
                                id,
                                blockchain_name,
                            );
                            Arc::new(SwitchClient::new(ws, http, upstream_state))
                        }
                        (Some(ws), None) => ws,
                        (None, Some(http)) => http,
                        (None, None) => {
                            tracing::warn!("Upstream {} has no RPC or WS configured, skipping", id);
                            continue;
                        }
                    };
                    let reader: Arc<dyn RpcUpstream> = Arc::new(OverloadGuard::new(reader));

                    // Validation probes the transport directly (`reader` at
                    // this point), below the method-filter and cache wrappers
                    // added later: a user's method allow-list must not fail
                    // the probes, and a cached answer must not pass them.
                    if options.disable_validation {
                        tracing::warn!("Disable validation for upstream {}", id);
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
                    //   HardcodedMethods → CachingUpstream → MethodFilter
                    //     → RequestLimitGuard → transport
                    // Hardcoded responses are cheapest, then cache, then network.
                    // Only calls that reach the network spend the request
                    // limits. Requests reach it already prepared by the
                    // chain's planner, e.g. rewritten to block-by-hash, which
                    // the cache can answer.
                    let caches = chains.caches(chain);
                    let reader = RequestLimitGuard::wrap(
                        reader,
                        RequestBudget::new(&upstream.limits).as_ref(),
                        &id,
                        chain,
                    );
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(MethodFilter::new(reader, Arc::clone(&methods)));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(CachingUpstream::new(reader, caches, EthereumCacheCodec));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(HardcodedMethods::new(reader, Arc::clone(&methods)));

                    let fork = fork_head.map(|head| ForkMember {
                        id: id.clone(),
                        priority: options.priority,
                        head,
                        state: Arc::clone(reader.state()),
                    });

                    // Outermost layer: attach the configured labels, capabilities,
                    // and routing role so `Describe` and upstream selection can
                    // read them.
                    let reader: Arc<dyn RpcUpstream> = Arc::new(IdentifiedUpstream::new(
                        reader,
                        vec![labels.clone()],
                        local_capabilities(options.balance),
                        upstream.role,
                    ));

                    chains.join(
                        chain,
                        ChainMember {
                            reader,
                            methods,
                            fork,
                        },
                    );
                }
                UpstreamConnection::Bitcoin(btc) => {
                    let labels = local_labels(upstream)?;
                    let chain = match parse_required_chain(upstream) {
                        Some(c) => c,
                        None => continue,
                    };
                    let blockchain_name = upstream.blockchain.as_deref().unwrap_or("?");
                    // Resolved before the transport: `options.timeout` bounds
                    // every call the client makes.
                    let options = config.options_for(upstream);

                    let rpc = match &btc.rpc {
                        Some(rpc) => rpc,
                        None => {
                            tracing::warn!("Upstream {} has no RPC configured, skipping", id);
                            continue;
                        }
                    };

                    let tls = crate::tls::client_tls(id.as_str(), rpc.tls.as_ref(), config_dir)?;
                    let client = crate::tls::reqwest_client(
                        tls.as_ref(),
                        options.timeout,
                        rpc.compress.unwrap_or(false),
                    )?;

                    tracing::info!(
                        "Using Bitcoin HTTP upstream '{}' at {} for {}",
                        id,
                        rpc.url,
                        blockchain_name,
                    );

                    let http_up = BitcoinHttpUpstream::new(
                        id.clone(),
                        rpc.url.clone(),
                        rpc.basic_auth.clone(),
                        client,
                    );
                    let head = http_up.head_height();
                    chains.cache_blocks_of(chain, &head);
                    let metered = MeteredUpstream::new(
                        Arc::new(http_up),
                        crate::metrics::UpstreamProtocol::HttpJsonRpc,
                        id.clone(),
                        chain,
                    );
                    let reader: Arc<dyn RpcUpstream> = Arc::new(LoggedUpstream::new(
                        Arc::new(metered),
                        crate::logs::Channel::JsonRpc,
                        id.clone(),
                        chain,
                    ));
                    let reader: Arc<dyn RpcUpstream> = Arc::new(OverloadGuard::new(reader));
                    let fork_head = Arc::clone(&head);
                    start_btc_head_poller(Arc::clone(&reader), Arc::clone(&head));

                    if let Some(esplora) = &btc.esplora
                        && let Entry::Vacant(slot) = per_chain_esplora.entry(chain)
                    {
                        let tls =
                            crate::tls::client_tls(id.as_str(), esplora.tls.as_ref(), config_dir)?;
                        let client = crate::tls::reqwest_client(
                            tls.as_ref(),
                            options.timeout,
                            esplora.compress.unwrap_or(false),
                        )?;
                        tracing::info!(
                            "Using Esplora at {} of upstream '{}' for {} balances",
                            esplora.url,
                            id,
                            blockchain_name,
                        );
                        let client =
                            EsploraClient::new(&esplora.url, esplora.basic_auth.clone(), client)
                                .with_context(|| format!("Upstream {id}"))?;
                        slot.insert(Arc::new(client));
                    }

                    // ZMQ push notifications complement the poller; both feed
                    // the same head, which dedups the overlap by block hash.
                    if let Some(zeromq) = &btc.zeromq {
                        let (host, port) = zeromq.host_port()?;
                        start_zmq_head(host.clone(), port, Arc::clone(&reader), head);
                        // Configured topics become egress subscriptions. A
                        // repeated topic gets one stream, not two — legacy
                        // collapsed repeats into an EnumMap, and a second
                        // stream would double every delivered notification.
                        let mut topics: Vec<BitcoinZmqTopic> = Vec::new();
                        for topic_id in &zeromq.topics {
                            match BitcoinZmqTopic::find_by_id(topic_id) {
                                Some(topic) if topics.contains(&topic) => {}
                                Some(topic) => {
                                    topics.push(topic);
                                    per_chain_zmq_topics
                                        .entry(chain)
                                        .or_default()
                                        .push(Arc::new(ZmqTopicStream::new(
                                            &host,
                                            port,
                                            topic,
                                            id.clone(),
                                        )));
                                }
                                None => tracing::error!("ZeroMQ topic is unknown: {}", topic_id),
                            }
                        }
                    }

                    // See the Ethereum branch for why validation targets the
                    // bare transport.
                    if options.disable_validation {
                        tracing::warn!("Disable validation for upstream {}", id);
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

                    // Same wrapping order as the Ethereum branch.
                    let caches = chains.caches(chain);
                    let reader = RequestLimitGuard::wrap(
                        reader,
                        RequestBudget::new(&upstream.limits).as_ref(),
                        &id,
                        chain,
                    );
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(MethodFilter::new(reader, Arc::clone(&methods)));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(CachingUpstream::new(reader, caches, BitcoinCacheCodec));
                    let reader: Arc<dyn RpcUpstream> =
                        Arc::new(HardcodedMethods::new(reader, Arc::clone(&methods)));

                    let fork = Some(ForkMember {
                        id: id.clone(),
                        priority: options.priority,
                        head: fork_head,
                        state: Arc::clone(reader.state()),
                    });

                    // Outermost layer: configured labels + capabilities for
                    // `Describe`, plus the routing role.
                    let reader: Arc<dyn RpcUpstream> = Arc::new(IdentifiedUpstream::new(
                        reader,
                        vec![labels.clone()],
                        local_capabilities(options.balance),
                        upstream.role,
                    ));

                    chains.join(
                        chain,
                        ChainMember {
                            reader,
                            methods,
                            fork,
                        },
                    );
                }
                UpstreamConnection::Dshackle(ds) => {
                    let tls = crate::tls::client_tls(id.as_str(), ds.tls.as_ref(), config_dir)?;
                    let Some(endpoint) =
                        RemoteEndpoint::new(ds, tls).with_context(|| format!("upstream {id}"))?
                    else {
                        tracing::warn!(
                            "Upstream {}: no URL or host configured for Dshackle connection, skipping",
                            id
                        );
                        continue;
                    };

                    // Only `timeout` applies to a dshackle connection; the
                    // validation family is meaningless here and gets a warning.
                    warn_inapplicable_dshackle_options(upstream);
                    // A relay serves whatever chains, labels, and methods its
                    // remote advertises, so locally configured ones have
                    // nothing to attach to (legacy warns about labels and
                    // silently ignores methods; ignoring config silently is
                    // worse than a warning).
                    if !upstream.labels.is_empty() {
                        tracing::warn!(
                            "Upstream {}: labels should be not applied to gRPC upstream",
                            id
                        );
                    }
                    if upstream.methods.is_some() {
                        tracing::warn!(
                            "Upstream {}: a methods config is not applied to a gRPC upstream and is ignored (the remote defines its own methods)",
                            id
                        );
                    }
                    let options = config.options_for(upstream);

                    // Defer the actual connect: it is done concurrently after the
                    // loop so a slow remote can't stall the rest of startup.
                    if endpoint.exposes_credentials() {
                        tracing::warn!(
                            "Upstream {}: the auth token is sent to {} without TLS, anyone on the network path can read it",
                            id,
                            endpoint.uri()
                        );
                    }
                    remotes.push(Arc::new(RemoteDshackle {
                        id,
                        role: upstream.role,
                        endpoint,
                        call_timeout: options.timeout,
                        priority: options.priority,
                        budget: RequestBudget::new(&upstream.limits),
                    }));
                }
            }
        }

        // Each remote is either connected and its chains added, or keeps
        // trying in the background, so a single unreachable one doesn't block
        // the start.
        let remotes_pending = !remotes.is_empty();
        futures::future::join_all(
            remotes
                .into_iter()
                .map(|remote| remote.start(Arc::clone(&chains))),
        )
        .await;

        let served = chains.list();
        if served.is_empty() && !remotes_pending {
            tracing::warn!("No usable upstreams were configured");
        }
        for chain in served {
            let count = chains.multistream(&chain).map_or(0, |m| m.len());
            if count > 1 {
                tracing::info!("{}: aggregating {} upstreams", chain, count);
            }
        }

        status::start_status_reporter(Arc::clone(&chains));

        Ok(UpstreamManager {
            chains,
            tokens: build_token_registry(tokens),
            esplora: per_chain_esplora,
            zmq_topics: per_chain_zmq_topics,
        })
    }

    /// Direct constructor for callers that already hold the resolved
    /// per-chain `Multistream`s and `Caches` — primarily code that wires
    /// upstreams outside the standard config-driven path (e.g. tests).
    pub(crate) fn from_parts(
        upstreams: HashMap<TargetBlockchain, Arc<Multistream>>,
        caches: HashMap<TargetBlockchain, Arc<Caches>>,
    ) -> Self {
        Self::from_chains(Arc::new(Chains::from_parts(upstreams, caches)))
    }

    /// Upstreams of the chains, with no tokens or Bitcoin-specific sources.
    pub(crate) fn from_chains(chains: Arc<Chains>) -> Self {
        Self {
            chains,
            tokens: HashMap::new(),
            esplora: HashMap::new(),
            zmq_topics: HashMap::new(),
        }
    }

    /// All chains with at least one upstream, sorted by chain id for
    /// deterministic `Describe` output (legacy `MultistreamHolder.getAvailable`).
    /// Grows when a remote Dshackle connects after the start.
    pub fn chains(&self) -> Vec<TargetBlockchain> {
        self.chains.list()
    }

    /// Look up the upstream aggregate for a given blockchain.
    pub fn get(&self, chain: &TargetBlockchain) -> Option<Arc<Multistream>> {
        self.chains.multistream(chain)
    }

    /// Look up the merged head stream for a given blockchain.
    pub fn head(&self, chain: &TargetBlockchain) -> Option<Arc<MergedHead>> {
        self.chains.head(chain)
    }

    /// The chain's pending transactions, or `None` when none of its upstreams
    /// can announce them: that takes a node reached over WebSocket, or a
    /// remote Dshackle relaying the topic.
    pub fn pending_txes(&self, chain: &TargetBlockchain) -> Option<Arc<dyn PendingTxSource>> {
        self.chains.pending_txes(chain)
    }

    /// Build the server-pushed-subscription egress for a chain, or `None` when
    /// the chain can't serve subscriptions. Ethereum-family chains serve the
    /// `eth_subscribe` topics; Bitcoin chains serve the ZMQ topics their
    /// upstreams were configured with (`zeromq.topics`). On top of the local
    /// topics, whatever a remote Dshackle upstream advertised over `Describe`
    /// is relayed (legacy `*DshackleIngressSubscription`).
    pub fn egress(&self, chain: &TargetBlockchain) -> Option<Arc<dyn EgressSubscription>> {
        let mut sources: Vec<Arc<dyn EgressSubscription>> = Vec::new();
        let mut local_topics: Vec<String> = Vec::new();
        match chain.blockchain_type() {
            BlockchainType::Ethereum => {
                if let (Some(head), Some(multistream)) = (self.head(chain), self.get(chain)) {
                    let access: Arc<dyn ChainAccess> = multistream;
                    let local =
                        Arc::new(EthereumEgress::new(head, access, self.pending_txes(chain)));
                    local_topics = local.available_topics();
                    sources.push(local);
                }
            }
            BlockchainType::Bitcoin => {
                if let Some(zmq) = self.zmq_topics.get(chain) {
                    sources.push(Arc::new(BitcoinEgress::new(zmq.clone())));
                }
            }
            BlockchainType::Unknown => {}
        }
        for relay in self.chains.relays(chain) {
            let added: Vec<String> = relay
                .available_topics()
                .into_iter()
                .filter(|topic| !local_topics.contains(topic))
                .collect();
            if added.is_empty() {
                continue;
            }
            sources.push(Arc::new(TopicSubset::new(relay as _, added)));
        }
        MergedEgress::combined(sources)
    }

    /// Build the fee estimator for a chain, or `None` when fee estimation isn't
    /// supported there. Ethereum-family chains read per-transaction fee fields
    /// (the EIP-1559 / legacy response shape is chosen per chain, mirroring the
    /// legacy `EthereumMultistream` `supportsEIP1559`); Bitcoin derives fees from
    /// input/output amounts via the block reader (legacy `BitcoinFees`).
    pub fn fees(&self, chain: &TargetBlockchain) -> Option<Arc<dyn ChainFees>> {
        match chain.blockchain_type() {
            BlockchainType::Ethereum => {
                let access: Arc<dyn ChainAccess> = self.get(chain)?;
                Some(Arc::new(EthereumFees::new(
                    access,
                    supports_eip1559(chain),
                    ETHEREUM_FEE_HEIGHT_LIMIT,
                )))
            }
            BlockchainType::Bitcoin => Some(Arc::new(BitcoinFees::new(
                self.bitcoin_reader(chain)?,
                BITCOIN_FEE_HEIGHT_LIMIT,
            ))),
            BlockchainType::Unknown => None,
        }
    }

    /// Build the balance stream for a `GetBalance` / `SubscribeBalance` request.
    /// `subscribe` selects streaming-on-change vs a one-shot current value. The
    /// tracker is chosen by chain + asset, mirroring the legacy
    /// `trackAddress.find { it.isSupported(request) }`: native Ether
    /// (`TrackEthereumAddress`), ERC-20 by token code or contract
    /// (`TrackERC20Address`), and native Bitcoin (`TrackBitcoinAddress`).
    pub async fn balance(
        &self,
        request: &BalanceRequest,
        subscribe: bool,
    ) -> Result<BalanceStream, BalanceError> {
        use emerald_api::proto::blockchain::balance_request::BalanceType;

        let chain = balance::request_chain(request)?;

        // A local Esplora index outranks every other Bitcoin balance source
        // (legacy `CurrentUnspentReader`), under the same gate as the rest of
        // the local path: an upstream of the chain is marked `balance: true`.
        let esplora = self
            .esplora
            .get(&chain)
            .filter(|_| self.chain_provides_balance(&chain));

        // Bitcoin: when a remote Dshackle advertises the BALANCE capability it
        // serves `GetBalance` directly, so forward to it rather than compute from
        // local UTXO RPC (legacy `CurrentUnspentReader` → `RemoteUnspentReader`).
        if chain.blockchain_type() == BlockchainType::Bitcoin
            && esplora.is_none()
            && let Some(client) = self.chains.balance_client(&chain)
        {
            return self
                .remote_balance(client, request.clone(), subscribe)
                .await;
        }

        let access: Arc<dyn ChainAccess> = self
            .get(&chain)
            .ok_or(BalanceError::Unavailable(chain.id()))?
            .clone();
        let head = self.head(&chain);
        let chain_id = chain.id();
        let eth = chain.blockchain_type() == BlockchainType::Ethereum;
        let btc = chain.blockchain_type() == BlockchainType::Bitcoin;
        // An xpub resolution is scanned by the Bitcoin arm below; every other
        // asset treats it as no match — legacy returns an empty, successful
        // stream rather than an error.
        let resolved = balance::resolve_addresses(&request.address)?;

        // `subscribe` vs one-shot over the chosen tracker (trackers share the
        // method shape but are distinct types, so the choice is made per arm).
        let stream = match request.balance_type.as_ref() {
            // Native Ether.
            Some(BalanceType::Asset(asset)) if eth && asset.code.eq_ignore_ascii_case("ether") => {
                let balance::ResolvedAddresses::List(addresses) = resolved else {
                    return Ok(balance::empty_stream());
                };
                let addresses = balance::parse_eth_addresses(addresses)?;
                // Legacy always reports the native asset code as "ETHER".
                let asset = emerald_api::proto::common::Asset {
                    chain: asset.chain,
                    code: "ETHER".to_string(),
                };
                let t = EthereumBalance::native(access, head, asset);
                if subscribe {
                    t.subscribe(addresses)
                } else {
                    t.get_balance(addresses)
                }
            }
            // ERC-20 named by a configured token code.
            Some(BalanceType::Asset(asset)) if eth => {
                let contract = self
                    .token_contract(&chain, &asset.code)
                    .ok_or(BalanceError::Unsupported)?;
                let balance::ResolvedAddresses::List(addresses) = resolved else {
                    return Ok(balance::empty_stream());
                };
                let addresses = balance::parse_eth_addresses(addresses)?;
                let t = EthereumBalance::erc20_named(
                    access,
                    head,
                    chain_id,
                    asset.code.clone(),
                    contract,
                );
                if subscribe {
                    t.subscribe(addresses)
                } else {
                    t.get_balance(addresses)
                }
            }
            // ERC-20 by contract address (any valid address, no config needed).
            Some(BalanceType::Erc20Asset(erc20)) if eth => {
                if !balance::is_valid_eth_address(&erc20.contract_address) {
                    return Err(BalanceError::Unsupported);
                }
                let balance::ResolvedAddresses::List(addresses) = resolved else {
                    return Ok(balance::empty_stream());
                };
                let addresses = balance::parse_eth_addresses(addresses)?;
                let t = EthereumBalance::erc20_contract(
                    access,
                    head,
                    chain_id,
                    erc20.contract_address.clone(),
                );
                if subscribe {
                    t.subscribe(addresses)
                } else {
                    t.get_balance(addresses)
                }
            }
            // Native Bitcoin (bitcoin / btc / satoshi).
            Some(BalanceType::Asset(asset))
                if btc
                    && matches!(
                        asset.code.to_lowercase().as_str(),
                        "bitcoin" | "btc" | "satoshi"
                    ) =>
            {
                // Legacy `isBalanceAvailable`: an upstream must advertise the
                // BALANCE capability for the local UTXO path to run.
                if !self.chain_provides_balance(&chain) {
                    return Err(BalanceError::Unsupported);
                }
                let source: Arc<dyn UnspentSource> = match esplora {
                    Some(client) => Arc::new(EsploraUnspentSource::new(Arc::clone(client))),
                    None => Arc::new(RpcUnspentSource::new(access)),
                };
                let mut addresses = match resolved {
                    balance::ResolvedAddresses::List(addresses) => addresses,
                    // An xpub address: derive and scan for the addresses in
                    // use (see `balance::xpub_scan`), then answer from the
                    // scan or watch the found set.
                    balance::ResolvedAddresses::Xpub(xpub) => {
                        let scan_request = balance::xpub_scan::ScanRequest::parse(&xpub, chain)?;
                        let found =
                            match balance::xpub_scan::scan(source.as_ref(), &scan_request).await {
                                Ok(found) => found,
                                Err(status) => return Ok(balance::error_stream(status)),
                            };
                        let t = BitcoinBalance::new(source, head, chain_id, request.include_utxo);
                        return Ok(if subscribe {
                            t.subscribe_scanned(found)
                        } else {
                            t.get_balance_scanned(found)
                        });
                    }
                };
                addresses.sort(); // legacy sorts multi addresses for Bitcoin
                let addresses = balance::bitcoin::validate_addresses(addresses, chain)?;
                let t = BitcoinBalance::new(source, head, chain_id, request.include_utxo);
                if subscribe {
                    t.subscribe(addresses)
                } else {
                    t.get_balance(addresses)
                }
            }
            _ => return Err(BalanceError::Unsupported),
        };
        Ok(stream)
    }

    /// Forward a `GetBalance` / `SubscribeBalance` request to a remote Dshackle
    /// upstream advertising the BALANCE capability, relaying its stream (legacy
    /// `RemoteUnspentReader`). `subscribe` selects the remote's streaming method.
    async fn remote_balance(
        &self,
        mut client: RemoteClient,
        request: BalanceRequest,
        subscribe: bool,
    ) -> Result<BalanceStream, BalanceError> {
        let response = if subscribe {
            client.subscribe_balance(request).await
        } else {
            client.get_balance(request).await
        }
        .map_err(BalanceError::Remote)?;
        Ok(Box::pin(response.into_inner()))
    }

    /// The contract address of an ERC-20 token configured under `name` for
    /// `chain` (case-insensitive), if any.
    fn token_contract(&self, chain: &TargetBlockchain, name: &str) -> Option<String> {
        self.tokens.get(&(*chain, name.to_lowercase())).cloned()
    }

    /// Whether any upstream on `chain` advertises the BALANCE capability — the
    /// gate the legacy `TrackBitcoinAddress.isBalanceAvailable` applies before
    /// serving a Bitcoin balance from local UTXO data.
    fn chain_provides_balance(&self, chain: &TargetBlockchain) -> bool {
        self.get(chain).is_some_and(|ms| {
            ms.upstreams()
                .iter()
                .any(|u| u.capabilities().contains(&Capability::Balance))
        })
    }

    /// Build the `SubscribeTxStatus` stream for a request, choosing the reader by
    /// chain (legacy `trackTx.find { isSupported }`). The confirmation limit is
    /// clamped per chain (Ethereum `[1, 100]`, Bitcoin `[1, 12]`).
    ///
    /// Bitcoin's clamp intentionally differs from a latent legacy bug: legacy
    /// computes `max(min(1, limit), 12)`, which collapses to a constant 12; we
    /// honor the client's requested limit within `[1, 12]`.
    pub fn tx_status(&self, request: &TxStatusRequest) -> Result<TxStatusStream, TxStatusError> {
        let chain = TargetBlockchain::try_from(request.chain)
            .map_err(|_| TxStatusError::Unavailable(request.chain))?;
        let multistream = self
            .get(&chain)
            .ok_or(TxStatusError::Unavailable(request.chain))?
            .clone();
        let head = self.head(&chain);

        let (reader, limit, ttl): (Arc<dyn tx_status::TxReader>, u32, tx_status::Ttl) =
            match chain.blockchain_type() {
                BlockchainType::Ethereum => {
                    let access: Arc<dyn ChainAccess> = multistream;
                    (
                        Arc::new(EthereumTxReader::new(access, request.tx_id.clone())),
                        request.confirmation_limit.clamp(1, 100),
                        tx_status::ethereum::ttl(),
                    )
                }
                BlockchainType::Bitcoin => {
                    let reader = self
                        .bitcoin_reader(&chain)
                        .ok_or(TxStatusError::Unsupported)?;
                    (
                        Arc::new(BitcoinTxReader::new(reader, request.tx_id.clone())),
                        request.confirmation_limit.clamp(1, 12),
                        tx_status::bitcoin::ttl(),
                    )
                }
                BlockchainType::Unknown => return Err(TxStatusError::Unsupported),
            };

        Ok(tx_status::subscribe(head, reader, limit, ttl))
    }

    /// Forward a `GetAddressAllowance` / `SubscribeAddressAllowance` request to a
    /// remote Dshackle upstream advertising the ALLOWANCE capability, relaying
    /// its stream (legacy `TrackERC20Allowance`). `subscribe` selects the
    /// remote's streaming method. With no such upstream the chain is
    /// unavailable, matching legacy.
    pub async fn address_allowance(
        &self,
        request: AddressAllowanceRequest,
        subscribe: bool,
    ) -> Result<AllowanceStream, AllowanceError> {
        let chain = TargetBlockchain::try_from(request.chain)
            .map_err(|_| AllowanceError::Unavailable(request.chain))?;
        let mut client = self
            .chains
            .allowance_client(&chain)
            .ok_or(AllowanceError::Unsupported(request.chain))?;

        let response = if subscribe {
            client.subscribe_address_allowance(request).await
        } else {
            client.get_address_allowance(request).await
        }
        .map_err(AllowanceError::Remote)?;
        Ok(Box::pin(response.into_inner()))
    }

    /// Build the Bitcoin block/transaction reader for a chain, or `None` for a
    /// non-Bitcoin chain. The data-access layer the Bitcoin fee estimator and
    /// address trackers read through (legacy `bitcoin/DataReaders`).
    pub fn bitcoin_reader(&self, chain: &TargetBlockchain) -> Option<BitcoinReader> {
        if chain.blockchain_type() != BlockchainType::Bitcoin {
            return None;
        }
        let access: Arc<dyn ChainAccess> = self.get(chain)?;
        Some(BitcoinReader::new(access, self.caches(chain)))
    }

    /// Look up the cache for a given blockchain.
    pub fn caches(&self, chain: &TargetBlockchain) -> Option<Arc<Caches>> {
        self.chains.existing_caches(chain)
    }
}

impl crate::metrics::UpstreamsStatus for UpstreamManager {
    fn chains_status(&self) -> Vec<crate::metrics::ChainStatus> {
        self.chains
            .statuses()
            .into_iter()
            .map(|status| crate::metrics::ChainStatus {
                chain: status.chain,
                upstreams: status
                    .upstreams
                    .iter()
                    .map(|up| crate::metrics::UpstreamStatus {
                        id: up.id().to_string(),
                        availability: up.availability(),
                        lag: up.lag(),
                        height: up.head().current_height(),
                    })
                    .collect(),
            })
            .collect()
    }
}

/// How many blocks back a single Ethereum fee estimate may sample (legacy
/// `EthereumMultistream` passes 256).
const ETHEREUM_FEE_HEIGHT_LIMIT: u32 = 256;

/// How many blocks back a single Bitcoin fee estimate may sample (legacy
/// `BitcoinMultistream` passes 6).
const BITCOIN_FEE_HEIGHT_LIMIT: u32 = 6;

/// Index the configured ERC-20 tokens by `(chain, lowercased name)` →
/// lowercased contract address, so a `GetBalance` request can name a token by
/// its code. Legacy `TrackERC20Address.init`.
fn build_token_registry(tokens: &[TokenConfig]) -> HashMap<(TargetBlockchain, String), String> {
    let mut registry = HashMap::new();
    for token in tokens {
        match token.token_type {
            TokenType::Erc20 => {
                registry.insert(
                    (token.blockchain, token.name.to_lowercase()),
                    token.address.to_lowercase(),
                );
            }
        }
    }
    registry
}

/// Whether the chain produces EIP-1559 (type-2) transactions, selecting the
/// extended fee response. Matches the legacy `ChainOptions.supportsEIP1559`
/// allow-list (mainnet plus the active PoS testnets — notably not Ethereum
/// Classic or the sidechains).
fn supports_eip1559(chain: &TargetBlockchain) -> bool {
    matches!(
        chain,
        TargetBlockchain::Standard(
            ChainRef::ChainEthereum
                | ChainRef::ChainGoerli
                | ChainRef::ChainHolesky
                | ChainRef::ChainSepolia
                | ChainRef::ChainHoodi
        )
    )
}

// ─── Helpers ───────────────────────────────────────────────────────────────

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

/// Capabilities for a locally-connected upstream: RPC always, plus BALANCE when
/// the operator marked it a balance provider (`balance: true`). Mirrors the
/// legacy `EthereumUpstream` / `BitcoinRpcUpstream` capability sets; ALLOWANCE
/// is never advertised by a local upstream.
fn local_capabilities(provides_balance: bool) -> Vec<Capability> {
    let mut caps = vec![Capability::Rpc];
    if provides_balance {
        caps.push(Capability::Balance);
    }
    caps
}

/// Map a remote Dshackle's reported `Capabilities` proto values to our
/// [`Capability`]. Unknown / `CAP_NONE` values are ignored. Mirrors the legacy
/// `RemoteCapabilities.extract`.
fn capabilities_from_proto(proto: &[i32]) -> Vec<Capability> {
    use emerald_api::proto::blockchain::Capabilities;
    proto
        .iter()
        .filter_map(|c| Capabilities::try_from(*c).ok())
        .filter_map(Capability::from_proto)
        .collect()
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
/// How a chain's calls are prepared for routing (see [`CallPlanner`]).
fn planner_for(chain: TargetBlockchain, caches: Arc<Caches>) -> Arc<dyn CallPlanner> {
    match chain.blockchain_type() {
        BlockchainType::Ethereum => Arc::new(EthereumCallPlanner::new(caches)),
        BlockchainType::Bitcoin | BlockchainType::Unknown => Arc::new(AsIs),
    }
}

/// Configured options that have no effect on a dshackle connection, by their
/// YAML names. The remote validates its own upstreams and reports availability
/// over `SubscribeStatus` (as in legacy), so the local validation settings are
/// meaningless there.
fn inapplicable_dshackle_options(
    options: &crate::config::upstreams::PartialOptions,
) -> Vec<&'static str> {
    [
        ("disable-validation", options.disable_validation.is_some()),
        ("validate-syncing", options.validate_syncing.is_some()),
        ("validate-peers", options.validate_peers.is_some()),
        ("min-peers", options.min_peers.is_some()),
        ("validation-interval", options.validation_interval.is_some()),
    ]
    .iter()
    .filter(|(_, set)| *set)
    .map(|(name, _)| *name)
    .collect()
}

/// A likely configuration mistake is called out loudly instead of being
/// silently ignored (options that parse but do nothing are the riskiest
/// migration gap).
fn warn_inapplicable_dshackle_options(upstream: &crate::config::upstreams::Upstream) {
    let ignored = inapplicable_dshackle_options(&upstream.options);
    if !ignored.is_empty() {
        tracing::warn!(
            "Upstream {}: option(s) {} do not apply to a dshackle connection and are ignored (the remote validates its own upstreams)",
            upstream.id,
            ignored.join(", ")
        );
    }
}

/// The validated labels of a local upstream, with the failure attributed to
/// the upstream — unlike the id, the label error can't name its owner itself.
fn local_labels(upstream: &crate::config::upstreams::Upstream) -> anyhow::Result<UpstreamLabels> {
    upstream
        .upstream_labels()
        .with_context(|| format!("upstream {}", upstream.id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::PartialOptions;
    use dshackle::DshackleUpstream;
    use dshackle::connection::remote_client;
    use dshackle::subscribe::DshackleEgress;
    use emerald_api::creds::Credentials;
    use methods::DefaultMethods;
    use std::collections::HashSet;
    use std::time::Duration;

    /// Chosen by chain, so an Ethereum chain served only by remote Dshackles
    /// is prepared the same as one with local nodes.
    #[test]
    fn calls_are_planned_by_chain_type() {
        let balance = crate::jsonrpc::JsonRpcRequest::new(
            1,
            "eth_getBalance".into(),
            serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "0x64"]),
        );
        let plan_on = |chain: ChainRef| {
            planner_for(chain.into(), Arc::new(Caches::new()))
                .plan(&balance, &|| Some(1000))
                .block
        };

        assert_eq!(
            plan_on(ChainRef::ChainEthereum),
            Some(call_plan::BlockRead::State(100))
        );
        assert_eq!(plan_on(ChainRef::ChainBitcoin), None);
    }

    /// A client to nowhere — enough to build a relay whose topics are
    /// inspected without any remote call.
    fn dead_client() -> RemoteClient {
        let channel = tonic::transport::Endpoint::from_shared("http://127.0.0.1:1")
            .unwrap()
            .connect_lazy();
        remote_client(channel, Credentials::None)
    }

    fn relay(chain: TargetBlockchain, topics: &[&str]) -> Arc<DshackleEgress> {
        Arc::new(DshackleEgress::new(
            "remote".parse().unwrap(),
            chain.id(),
            dead_client(),
            topics.iter().map(|t| t.to_string()).collect(),
            Duration::from_secs(1),
        ))
    }

    fn chains_with_relay(chain: TargetBlockchain, topics: &[&str]) -> Chains {
        let chains = Chains::new(None);
        chains.add_relay(chain, relay(chain, topics));
        chains
    }

    fn manager_with_relay(chain: TargetBlockchain, topics: &[&str]) -> UpstreamManager {
        UpstreamManager::from_chains(Arc::new(chains_with_relay(chain, topics)))
    }

    /// Joins `upstream` to the chain with a tracked head, so the chain has a
    /// merged head as well.
    fn join_with_head(chains: &Chains, chain: TargetBlockchain, upstream: Arc<dyn RpcUpstream>) {
        let fork = ForkMember {
            id: upstream.id().clone(),
            priority: 0,
            head: Arc::new(CurrentHead::new()),
            state: Arc::clone(upstream.state()),
        };
        chains.join(
            chain,
            ChainMember {
                reader: upstream,
                methods: Arc::new(DefaultMethods),
                fork: Some(fork),
            },
        );
    }

    fn remote_upstream(chain: TargetBlockchain) -> Arc<dyn RpcUpstream> {
        Arc::new(DshackleUpstream::new(
            "remote_eth".parse().unwrap(),
            chain.id(),
            dead_client(),
            6,
            Duration::from_secs(1),
        ))
    }

    #[tokio::test]
    async fn egress_serves_remote_topics_without_local_sources() {
        // A Bitcoin chain with no local `zeromq.topics` still gets an egress
        // when a remote Dshackle advertises topics to relay.
        let chain: TargetBlockchain = "bitcoin".parse().unwrap();
        let manager = manager_with_relay(chain, &["hashtx", "hashblock"]);

        let egress = manager.egress(&chain).expect("relay-only egress");
        assert_eq!(
            egress.available_topics(),
            vec!["hashtx".to_string(), "hashblock".to_string()]
        );
    }

    #[tokio::test]
    async fn egress_cuts_locally_served_ethereum_topics_from_the_relay() {
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        let chains = chains_with_relay(chain, &["newHeads", "newPendingTransactions"]);
        join_with_head(&chains, chain, remote_upstream(chain));
        let manager = UpstreamManager::from_chains(Arc::new(chains));

        // Both of the remote's topics are served locally, so neither is
        // relayed on top: `newHeads` is derived from the same head the remote
        // feeds, and its pending transactions come through the chain's own
        // source, where they are merged with those of the other upstreams.
        let egress = manager.egress(&chain).expect("local egress");
        assert_eq!(
            egress.available_topics(),
            vec![
                "newHeads".to_string(),
                "syncing".to_string(),
                "logs".to_string(),
                "newPendingTransactions".to_string(),
            ]
        );
    }

    /// The whole path a subscriber depends on: a node announcing over its
    /// WebSocket connection, the chain's source, and the egress handed to the
    /// gRPC `NativeSubscribe` and the WS proxy.
    #[tokio::test]
    async fn egress_serves_pending_txes_announced_by_node() {
        use ethereum::ws_stub::StubNode;
        use tokio_stream::StreamExt;

        let node = StubNode::start().await;
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        let ws_upstream = Arc::new(EthereumWsUpstream::new(
            "node".parse().unwrap(),
            ethereum::WsTarget {
                url: node.url(),
                origin: None,
                basic_auth: None,
                call_timeout: Duration::from_secs(1),
                frame_size: None,
                msg_size: None,
            },
            1,
            Arc::new(UpstreamState::new()),
        ));

        let chains = Chains::new(None);
        join_with_head(
            &chains,
            chain,
            Arc::clone(&ws_upstream) as Arc<dyn RpcUpstream>,
        );
        chains.add_pending_source(chain, Arc::new(WsPendingTxes::new(ws_upstream)));
        let manager = UpstreamManager::from_chains(Arc::new(chains));

        let egress = manager.egress(&chain).expect("ethereum egress");
        assert!(
            egress
                .available_topics()
                .contains(&"newPendingTransactions".to_string())
        );
        let mut stream = egress.subscribe("newPendingTransactions", None).unwrap();
        node.requests("eth_subscribe", 1).await;

        let hash = format!("0x{}", "ab".repeat(32));
        node.notify(serde_json::json!(hash));
        let message = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a pending transaction in time")
            .expect("an open stream");
        assert_eq!(message, format!("\"{hash}\"").into_bytes());
    }

    #[tokio::test]
    async fn pending_txes_come_from_relays_advertising_them() {
        let ethereum: TargetBlockchain = "ethereum".parse().unwrap();
        let sepolia: TargetBlockchain = "sepolia".parse().unwrap();
        let chains = Chains::new(None);

        chains.add_relay(
            ethereum,
            relay(ethereum, &["newHeads", "newPendingTransactions"]),
        );
        chains.add_relay(sepolia, relay(sepolia, &["newHeads"]));

        assert!(chains.pending_txes(&ethereum).is_some());
        assert!(chains.pending_txes(&sepolia).is_none());
    }

    /// An Ethereum chain whose only upstream is a remote Dshackle that does
    /// NOT expose `eth_getLogs` (its Describe method list gates local calls).
    fn chain_without_get_logs(chains: &Chains, chain: TargetBlockchain) {
        let callable: HashSet<crate::jsonrpc::RpcMethod> = ["eth_blockNumber"]
            .iter()
            .filter_map(|m| m.parse().ok())
            .collect();
        let upstream: Arc<dyn RpcUpstream> = Arc::new(MethodFilter::new(
            remote_upstream(chain),
            Arc::new(ConfiguredMethods::allowed_only(callable)),
        ));
        join_with_head(chains, chain, upstream);
    }

    #[tokio::test]
    async fn egress_hides_logs_when_get_logs_is_not_callable() {
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        let chains = Chains::new(None);
        chain_without_get_logs(&chains, chain);
        let manager = UpstreamManager::from_chains(Arc::new(chains));

        let egress = manager.egress(&chain).expect("local egress");
        assert_eq!(
            egress.available_topics(),
            vec!["newHeads".to_string(), "syncing".to_string()]
        );
    }

    #[tokio::test]
    async fn egress_relays_logs_when_local_get_logs_is_not_callable() {
        // The local egress doesn't claim `logs` here (see the test above), so
        // the cut leaves the relay's `logs` in place and subscribers reach
        // the remote's working stream instead of a silent local one.
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        let chains = chains_with_relay(chain, &["logs"]);
        chain_without_get_logs(&chains, chain);
        let manager = UpstreamManager::from_chains(Arc::new(chains));

        let egress = manager.egress(&chain).expect("local + relay egress");
        assert_eq!(
            egress.available_topics(),
            vec![
                "newHeads".to_string(),
                "syncing".to_string(),
                "logs".to_string(),
            ]
        );
        assert!(egress.subscribe("logs", None).is_ok());
    }

    #[tokio::test]
    async fn egress_relays_ethereum_topics_when_nothing_is_served_locally() {
        // Without a tracked head there is no local egress, so everything the
        // remote advertises is relayed, head-derived topics included.
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        let manager = manager_with_relay(chain, &["newHeads", "newPendingTransactions"]);

        let egress = manager.egress(&chain).expect("relay-only egress");
        assert_eq!(
            egress.available_topics(),
            vec!["newHeads".to_string(), "newPendingTransactions".to_string()]
        );
    }

    #[tokio::test]
    async fn upstream_joining_later_is_routed_with_the_others() {
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        let chains = Arc::new(Chains::new(None));
        join_with_head(&chains, chain, remote_upstream(chain));
        let manager = UpstreamManager::from_chains(Arc::clone(&chains));
        let routing = manager.get(&chain).unwrap();

        let late: Arc<dyn RpcUpstream> = Arc::new(DshackleUpstream::new(
            "late_eth".parse().unwrap(),
            chain.id(),
            dead_client(),
            6,
            Duration::from_secs(1),
        ));
        join_with_head(&chains, chain, late);

        // The same routing as before, so whoever holds it sees the newcomer
        assert_eq!(routing.len(), 2);
        assert_eq!(manager.chains(), vec![chain]);
    }

    #[tokio::test]
    async fn chain_joining_later_is_served() {
        let manager = UpstreamManager::from_chains(Arc::new(Chains::new(None)));
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        assert!(manager.get(&chain).is_none());

        join_with_head(&manager.chains, chain, remote_upstream(chain));

        assert_eq!(manager.chains(), vec![chain]);
        assert!(manager.head(&chain).is_some());
    }

    const BITCOIN: TargetBlockchain = TargetBlockchain::Standard(ChainRef::ChainBitcoin);

    // BIP173 example mainnet P2WPKH address.
    const BITCOIN_ADDRESS: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";

    /// A manager with a Bitcoin chain of one unreachable node, marked as a
    /// balance provider (`balance: true`) or not, plus an Esplora index at
    /// `esplora_url`.
    fn bitcoin_manager_with_esplora(provides_balance: bool, esplora_url: &str) -> UpstreamManager {
        let node: Arc<dyn RpcUpstream> = Arc::new(DshackleUpstream::new(
            "node".parse().unwrap(),
            BITCOIN.id(),
            dead_client(),
            6,
            Duration::from_secs(1),
        ));
        let node: Arc<dyn RpcUpstream> = Arc::new(IdentifiedUpstream::new(
            node,
            Vec::new(),
            local_capabilities(provides_balance),
            crate::config::upstreams::UpstreamRole::Primary,
        ));
        let chains = Chains::new(None);
        chains.join(
            BITCOIN,
            ChainMember {
                reader: node,
                methods: Arc::new(DefaultBitcoinMethods::new()),
                fork: None,
            },
        );
        let mut manager = UpstreamManager::from_chains(Arc::new(chains));
        let http = crate::tls::reqwest_client(None, Duration::from_secs(5), false).unwrap();
        let esplora = EsploraClient::new(esplora_url, None, http).unwrap();
        manager.esplora.insert(BITCOIN, Arc::new(esplora));
        manager
    }

    fn bitcoin_balance_request() -> BalanceRequest {
        use emerald_api::proto::blockchain::balance_request::BalanceType;
        use emerald_api::proto::common::any_address::AddrType;
        use emerald_api::proto::common::{AnyAddress, Asset, SingleAddress};
        BalanceRequest {
            address: Some(AnyAddress {
                addr_type: Some(AddrType::AddressSingle(SingleAddress {
                    address: BITCOIN_ADDRESS.to_string(),
                })),
            }),
            include_utxo: false,
            balance_type: Some(BalanceType::Asset(Asset {
                chain: BITCOIN.id(),
                code: "BTC".to_string(),
            })),
        }
    }

    /// An Esplora that knows one 700 satoshi output on [`BITCOIN_ADDRESS`].
    async fn esplora_with_balance() -> bitcoin::esplora_stub::StubEsplora {
        let esplora = bitcoin::esplora_stub::StubEsplora::start().await;
        esplora.respond(
            &format!("/address/{BITCOIN_ADDRESS}/utxo"),
            r#"[{"txid":"aa","vout":0,"status":{"confirmed":true},"value":700}]"#,
        );
        esplora
    }

    async fn first_balance(manager: &UpstreamManager) -> String {
        use futures::StreamExt;
        let mut stream = manager
            .balance(&bitcoin_balance_request(), false)
            .await
            .map_err(BalanceError::into_status)
            .unwrap();
        stream.next().await.unwrap().unwrap().balance
    }

    // The node of these chains is unreachable, so an answer can only have
    // come from Esplora.

    #[tokio::test]
    async fn bitcoin_balance_is_read_from_esplora_when_configured() {
        let esplora = esplora_with_balance().await;
        let manager = bitcoin_manager_with_esplora(true, esplora.url());

        assert_eq!(first_balance(&manager).await, "700");
    }

    #[tokio::test]
    async fn esplora_outranks_a_remote_balance_provider() {
        let esplora = esplora_with_balance().await;
        let manager = bitcoin_manager_with_esplora(true, esplora.url());
        manager.chains.offer_balance_client(BITCOIN, &dead_client());

        assert_eq!(first_balance(&manager).await, "700");
    }

    #[tokio::test]
    async fn esplora_is_left_alone_without_a_balance_provider() {
        let esplora = esplora_with_balance().await;
        let manager = bitcoin_manager_with_esplora(false, esplora.url());

        let result = manager.balance(&bitcoin_balance_request(), false).await;

        assert!(matches!(result, Err(BalanceError::Unsupported)));
        assert!(esplora.requests().is_empty());
    }

    #[test]
    fn dshackle_options_all_applicable_by_default() {
        assert!(inapplicable_dshackle_options(&PartialOptions::default()).is_empty());
    }

    #[test]
    fn dshackle_options_flags_validation_settings() {
        let options = PartialOptions {
            min_peers: Some(3),
            validate_syncing: Some(true),
            ..Default::default()
        };
        assert_eq!(
            inapplicable_dshackle_options(&options),
            vec!["validate-syncing", "min-peers"]
        );
    }

    #[test]
    fn dshackle_options_timeout_is_applicable() {
        // `timeout` is the one option legacy applied to a dshackle connection —
        // it must not be flagged.
        let options = PartialOptions {
            timeout: Some(30),
            priority: Some(100),
            balance: Some(true),
            ..Default::default()
        };
        assert!(inapplicable_dshackle_options(&options).is_empty());
    }
}
