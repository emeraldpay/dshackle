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

//! A configured remote Dshackle upstream, and how its chains join the local
//! ones.

use super::DshackleUpstream;
use super::connection::{RemoteClient, RemoteEndpoint};
use super::head::start_head_subscriber;
use super::status::start_status_subscriber;
use super::subscribe::DshackleEgress;
use crate::blockchain::TargetBlockchain;
use crate::config::upstreams::UpstreamRole;
use crate::upstream::chains::{ChainMember, Chains};
use crate::upstream::fork::ForkMember;
use crate::upstream::id::UpstreamId;
use crate::upstream::identified::IdentifiedUpstream;
use crate::upstream::label::{UpstreamLabel, UpstreamLabels};
use crate::upstream::limits::{RequestBudget, RequestLimitGuard};
use crate::upstream::logged::LoggedUpstream;
use crate::upstream::metered::MeteredUpstream;
use crate::upstream::methods::{ConfiguredMethods, MethodFilter, RemoteMethods};
use crate::upstream::overload::OverloadGuard;
use crate::upstream::pause::PauseReason;
use crate::upstream::quorum::QuorumFactory;
use crate::upstream::traits::{Capability, RpcUpstream};
use crate::upstream::{capabilities_from_proto, syncing_lag_for};
use emerald_api::proto::blockchain::DescribeChain;
use emerald_api::proto::common::ChainRef;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// How long to wait for one attempt to connect and call `Describe`. A slow
/// remote must not hold back the whole server at the start, and must not stall
/// the retries after it.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The retries follow the pause of an upstream refusing calls (like one
/// answering HTTP 401): 10 seconds, doubled per failed attempt, up to 5 minutes.
const RETRY_BACKOFF: PauseReason = PauseReason::RateLimited;

pub struct RemoteDshackle {
    pub id: UpstreamId,
    pub role: UpstreamRole,
    pub endpoint: RemoteEndpoint,
    pub call_timeout: Duration,
    /// Merge priority of the discovered chains' heads, as for local upstreams.
    pub priority: i32,
    /// One budget for every chain the remote serves: the configured limits
    /// are for the connection to the remote, not for each of its chains.
    pub budget: Option<Arc<RequestBudget>>,
}

impl RemoteDshackle {
    /// Connects to the remote and adds the chains it serves. When it can't be
    /// reached, or rejects the auth token, it keeps trying in the background,
    /// as for any upstream that's unavailable, and adds them once it can.
    pub async fn start(self: Arc<Self>, chains: Arc<Chains>) {
        tracing::info!(
            "Connecting to remote Dshackle '{}' at {}",
            self.id,
            self.endpoint.uri()
        );
        match self.connect().await {
            Ok((client, described)) => self.join(&chains, client, described),
            Err(e) => {
                // `{:#}` so the whole chain shows: a bare tonic
                // `transport::Error` renders as just "transport error",
                // and the reason is only in its source.
                tracing::warn!(
                    "Upstream {}: failed to connect to Dshackle at {}, starting without it until it connects: {:#}",
                    self.id,
                    self.endpoint.uri(),
                    e
                );
                tokio::spawn(async move {
                    let (client, described) =
                        retry_until_connected(&self.id, || self.connect()).await;
                    self.join(&chains, client, described);
                });
            }
        }
    }

    async fn connect(&self) -> anyhow::Result<(RemoteClient, Vec<DescribeChain>)> {
        tokio::time::timeout(CONNECT_TIMEOUT, self.endpoint.connect_and_describe())
            .await
            .map_err(|_| anyhow::anyhow!("timed out after {CONNECT_TIMEOUT:?}"))?
    }

    /// Adds the chains the remote reported: one upstream per chain, with head
    /// and status subscribers, the methods the remote advertises, and its
    /// labels and capabilities.
    pub fn join(&self, chains: &Chains, client: RemoteClient, described: Vec<DescribeChain>) {
        if described.is_empty() {
            tracing::warn!(
                "Dshackle '{}': remote reported no available chains",
                self.id
            );
            return;
        }
        let mut connected = 0;
        for desc_chain in &described {
            if self.join_chain(chains, &client, desc_chain) {
                connected += 1;
            }
        }
        tracing::info!(
            "Dshackle '{}': connected with {} chain(s)",
            self.id,
            connected
        );
    }

    fn join_chain(
        &self,
        chains: &Chains,
        client: &RemoteClient,
        desc_chain: &DescribeChain,
    ) -> bool {
        let upstream_id = &self.id;
        let chain_ref = match ChainRef::try_from(desc_chain.chain) {
            Ok(c) if c != ChainRef::ChainUnspecified => c,
            _ => {
                tracing::debug!(
                    "Dshackle '{}': skipping unknown chain id {}",
                    upstream_id,
                    desc_chain.chain
                );
                return false;
            }
        };

        let chain = TargetBlockchain::from(chain_ref);
        // One upstream per discovered chain, named like the legacy
        // `GrpcUpstreams`: `<id>_<chain code>`.
        let chain_id: UpstreamId =
            match format!("{}_{}", upstream_id, chain.code().to_lowercase()).parse() {
                Ok(id) => id,
                Err(e) => {
                    // Only possible if a future chain code stops fitting the
                    // id pattern; losing one discovered chain beats panicking.
                    tracing::warn!("Dshackle '{upstream_id}': skipping chain {chain}: {e}");
                    return false;
                }
            };

        tracing::info!(
            "Dshackle '{}': discovered chain {} ({} methods)",
            upstream_id,
            chain,
            desc_chain.supported_methods.len(),
        );

        let ds_upstream = Arc::new(DshackleUpstream::new(
            chain_id.clone(),
            desc_chain.chain,
            client.clone(),
            syncing_lag_for(chain_ref),
            self.call_timeout,
        ));

        // Subscribe caching head before starting the poller so no blocks are missed
        let head = ds_upstream.head_height();
        chains.cache_blocks_of(chain, &head);

        // The remote's head joins the chain's fork members like any local
        // upstream's, so the chain gets a `MergedHead` even when every
        // upstream is a remote Dshackle: that serves the local
        // `SubscribeHead`, and the head-derived egress topics are produced
        // once from the merge instead of relayed per remote.
        let fork = ForkMember {
            id: chain_id.clone(),
            priority: self.priority,
            head,
            state: ds_upstream.state_handle(),
        };

        start_head_subscriber(chain_id.clone(), desc_chain.chain, Arc::clone(&ds_upstream));

        // Track the remote's own reported availability via SubscribeStatus.
        start_status_subscriber(
            chain_id.clone(),
            desc_chain.chain,
            ds_upstream.grpc_client(),
            ds_upstream.state_handle(),
        );

        let metered = MeteredUpstream::new(
            ds_upstream,
            crate::metrics::UpstreamProtocol::Dshackle,
            upstream_id.clone(),
            chain,
        );
        let reader: Arc<dyn RpcUpstream> = Arc::new(LoggedUpstream::new(
            Arc::new(metered),
            crate::logs::Channel::Dshackle,
            upstream_id.clone(),
            chain,
        ));
        // Pauses it when the remote refuses our credentials, as an HTTP
        // upstream answering 401.
        let reader: Arc<dyn RpcUpstream> = Arc::new(OverloadGuard::new(reader));
        let reader = RequestLimitGuard::wrap(reader, self.budget.as_ref(), upstream_id, chain);

        // Capabilities come from the remote's own report (legacy
        // `RemoteCapabilities.extract`), so a Dshackle relay re-advertises what
        // the backend can serve.
        let caps = capabilities_from_proto(&desc_chain.capabilities);

        // Forward allowance and balance requests to the remote when it
        // advertises the capability (legacy `allowanceUpstreamMatcher`): a
        // BALANCE-capable remote serves `GetBalance` directly, so Bitcoin
        // balance requests go to it.
        if caps.contains(&Capability::Allowance) {
            chains.offer_allowance_client(chain, client);
        }
        if caps.contains(&Capability::Balance) {
            chains.offer_balance_client(chain, client);
        }

        // Topics the remote advertises become a `NativeSubscribe` relay, so
        // local clients can subscribe to them too and the chain's own
        // `Describe` re-advertises them (legacy `*DshackleIngressSubscription`).
        if !desc_chain.supported_subscriptions.is_empty() {
            chains.add_relay(
                chain,
                Arc::new(DshackleEgress::new(
                    chain_id.clone(),
                    desc_chain.chain,
                    client.clone(),
                    desc_chain.supported_subscriptions.clone(),
                    self.call_timeout,
                )),
            );
        }

        // Use the supported methods from Describe as the allowed set.
        // The remote Dshackle already handles hardcoded responses, so we
        // only need a MethodFilter — no HardcodedMethods wrapper.
        let reader = if !desc_chain.supported_methods.is_empty() {
            let callable: HashSet<_> = desc_chain
                .supported_methods
                .iter()
                .filter_map(|m| m.parse().ok())
                .collect();
            let methods: Arc<dyn QuorumFactory> =
                Arc::new(ConfiguredMethods::allowed_only(callable));
            Arc::new(MethodFilter::new(reader, methods)) as Arc<dyn RpcUpstream>
        } else {
            // No method list — pass everything through
            reader
        };

        // Outermost layer: the capabilities the remote reported, plus one label
        // set per node the remote advertises (legacy `GrpcUpstreamStatus`) so
        // label selectors route through the relay without duplicating the
        // remote's labels in the local config.
        let label_sets: Vec<UpstreamLabels> = desc_chain
            .nodes
            .iter()
            .map(|node| {
                node.labels
                    .iter()
                    .filter_map(|l| match UpstreamLabel::new(&l.name, &l.value) {
                        Ok(label) => Some(label),
                        Err(e) => {
                            // A remote's labels are its own config's business,
                            // but an empty one can't be matched meaningfully —
                            // drop it rather than advertise it further.
                            tracing::warn!("Dshackle '{upstream_id}': dropping label: {e}");
                            None
                        }
                    })
                    .collect()
            })
            .collect();
        let reader: Arc<dyn RpcUpstream> =
            Arc::new(IdentifiedUpstream::new(reader, label_sets, caps, self.role));

        chains.join(
            chain,
            ChainMember {
                reader,
                // Remote Dshackles handle their own quorum internally, so this
                // keeps every method callable — but it carries the remote's
                // discovered methods so `Describe` re-advertises them instead
                // of nothing.
                methods: Arc::new(RemoteMethods::new(desc_chain.supported_methods.clone())),
                fork: Some(fork),
            },
        );
        true
    }
}

/// Repeats `connect` until it succeeds, waiting longer after each failure.
async fn retry_until_connected<T, F, Fut>(id: &UpstreamId, mut connect: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut failures = 0;
    loop {
        tokio::time::sleep(RETRY_BACKOFF.cooldown(failures)).await;
        match connect().await {
            Ok(connected) => return connected,
            Err(e) => {
                failures += 1;
                tracing::warn!(
                    "Upstream {}: still can't connect to Dshackle, next attempt in {}s: {:#}",
                    id,
                    RETRY_BACKOFF.cooldown(failures).as_secs(),
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::dshackle::connection::remote_client;
    use crate::upstream::dshackle::stub_remote::{StubRemote, TOKEN};
    use emerald_api::creds::Credentials;
    use std::sync::Mutex;
    use tokio::time::Instant;

    fn remote(url: &str, token: Option<&str>) -> Arc<RemoteDshackle> {
        let yaml = match token {
            Some(token) => format!("{{url: '{url}', emerald-auth: {{token: '{token}'}}}}"),
            None => format!("url: '{url}'"),
        };
        Arc::new(RemoteDshackle {
            id: "remote".parse().unwrap(),
            role: UpstreamRole::Primary,
            endpoint: RemoteEndpoint::new(&serde_yaml::from_str(&yaml).unwrap(), None)
                .unwrap()
                .unwrap(),
            call_timeout: Duration::from_secs(1),
            priority: 42,
            budget: None,
        })
    }

    fn ethereum() -> TargetBlockchain {
        TargetBlockchain::from(ChainRef::ChainEthereum)
    }

    fn dead_client() -> RemoteClient {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        remote_client(channel, Credentials::None)
    }

    #[tokio::test(start_paused = true)]
    async fn retries_with_growing_pauses_until_connected() {
        let started = Instant::now();
        let attempts = Mutex::new(Vec::new());

        let connected = retry_until_connected(&"remote".parse().unwrap(), || {
            let mut attempts = attempts.lock().unwrap();
            attempts.push(started.elapsed().as_secs());
            let result = if attempts.len() < 3 {
                Err(anyhow::anyhow!("rejected"))
            } else {
                Ok("connected")
            };
            async move { result }
        })
        .await;

        assert_eq!(connected, "connected");
        assert_eq!(*attempts.lock().unwrap(), vec![10, 30, 70]);
    }

    #[tokio::test]
    async fn discovered_chain_joins_with_its_head() {
        let chains = Chains::new(None);

        remote("http://127.0.0.1:1", None).join(
            &chains,
            dead_client(),
            vec![DescribeChain {
                chain: ChainRef::ChainEthereum as i32,
                ..Default::default()
            }],
        );

        let upstreams = chains
            .multistream(&ethereum())
            .expect("chain joined")
            .upstreams();
        assert_eq!(upstreams.len(), 1);
        assert_eq!(upstreams[0].id().as_str(), "remote_eth");
        // Even with no local upstream, the chain has a merged head
        assert!(chains.head(&ethereum()).is_some());
    }

    #[tokio::test]
    async fn accepted_token_at_start_joins_the_chains() {
        let stub = StubRemote::new();
        let url = stub.start().await;
        let chains = Arc::new(Chains::new(None));

        remote(&url, Some(TOKEN)).start(Arc::clone(&chains)).await;

        assert!(chains.multistream(&ethereum()).is_some());
    }

    #[tokio::test]
    async fn rejected_token_at_start_leaves_the_chains_for_later() {
        let stub = StubRemote::new();
        let url = stub.start().await;
        let chains = Arc::new(Chains::new(None));

        // Returns without waiting for the retries
        remote(&url, Some("wrong-token"))
            .start(Arc::clone(&chains))
            .await;

        assert!(chains.multistream(&ethereum()).is_none());
    }
}
