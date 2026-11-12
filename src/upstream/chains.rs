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

//! The per-chain parts shared by the upstreams of each chain: routing, the
//! merged head, fork detection, caches, and what remote Dshackles provide.
//!
//! An upstream may join a chain at any time, not only at the start: a remote
//! Dshackle that couldn't be reached then (or rejected the auth token) adds its
//! chains once it connects.

use super::dshackle::connection::RemoteClient;
use super::dshackle::subscribe::DshackleEgress;
use super::ethereum::pending_tx::{AggregatedPendingTxes, PendingTxSource, RelayedPendingTxes};
use super::fork::{ForkGroup, ForkMember, is_pos};
use super::lag;
use super::merged_head::{MergeOrder, MergedHead};
use super::multistream::Multistream;
use super::planner_for;
use super::quorum::QuorumFactory;
use super::status::ChainStatus;
use super::traits::RpcUpstream;
use crate::blockchain::TargetBlockchain;
use crate::cache::{Caches, CachingHead, RedisCache};
use crate::upstream::head::CurrentHead;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// What an upstream brings to the chain it joins.
pub struct ChainMember {
    pub reader: Arc<dyn RpcUpstream>,
    pub methods: Arc<dyn QuorumFactory>,
    /// Its head, for the chain's merged head and fork detection; none for an
    /// upstream whose head isn't tracked.
    pub fork: Option<ForkMember>,
}

pub struct Chains {
    redis: Option<redis::aio::ConnectionManager>,
    state: RwLock<State>,
}

#[derive(Default)]
struct State {
    multistreams: HashMap<TargetBlockchain, Arc<Multistream>>,
    heads: HashMap<TargetBlockchain, Arc<MergedHead>>,
    forks: HashMap<TargetBlockchain, Arc<ForkGroup>>,
    caches: HashMap<TargetBlockchain, Arc<Caches>>,
    /// Writes each new block of the chain to its caches once, whichever of its
    /// upstreams reports it first.
    caching_heads: HashMap<TargetBlockchain, CachingHead>,
    /// A remote Dshackle client per chain to forward allowance requests to: the
    /// first one advertising that capability (legacy picks one upstream
    /// matching the ALLOWANCE capability).
    allowance_clients: HashMap<TargetBlockchain, RemoteClient>,
    /// Same for balance requests (BALANCE capability).
    balance_clients: HashMap<TargetBlockchain, RemoteClient>,
    /// Subscription relays of the remote Dshackles, carrying the topics each
    /// advertised over `Describe` (legacy `*DshackleIngressSubscription`).
    /// Held here so all subscribers share one remote stream per topic.
    relays: HashMap<TargetBlockchain, Vec<Arc<DshackleEgress>>>,
    pending_sources: HashMap<TargetBlockchain, Vec<Arc<dyn PendingTxSource>>>,
    /// The chain's pending transactions, gathered from every source able to
    /// announce them, so all listeners share the upstream subscriptions and the
    /// memory of what was already announced (legacy `EthereumMultistream`).
    pending_txes: HashMap<TargetBlockchain, Arc<dyn PendingTxSource>>,
}

impl Chains {
    pub fn new(redis: Option<redis::aio::ConnectionManager>) -> Self {
        Self {
            redis,
            state: RwLock::new(State::default()),
        }
    }

    /// Chains served by already built routing, with no head, forks, or remote
    /// parts.
    pub fn from_parts(
        multistreams: HashMap<TargetBlockchain, Arc<Multistream>>,
        caches: HashMap<TargetBlockchain, Arc<Caches>>,
    ) -> Self {
        Self {
            redis: None,
            state: RwLock::new(State {
                multistreams,
                caches,
                ..State::default()
            }),
        }
    }

    /// Adds the upstream to its chain's routing, merged head, and fork
    /// detection, starting them if it's the first upstream there.
    pub fn join(&self, chain: TargetBlockchain, member: ChainMember) {
        let mut state = self.state.write().unwrap();
        if let Some(fork) = member.fork {
            // The merged head follows every upstream independently of the fork
            // verdicts (same as the legacy `MergedPow/PosHead`): conflicts are
            // resolved inside the merge, and any upstream can advance the head,
            // so one stalled upstream can never stall the chain.
            state
                .heads
                .entry(chain)
                .or_insert_with(|| Arc::new(MergedHead::new(merge_order(chain))))
                .follow(fork.priority, Arc::clone(&fork.head));
            state
                .forks
                .entry(chain)
                .or_insert_with(|| ForkGroup::new(chain))
                .join(fork);
        }
        if let Some(multistream) = state.multistreams.get(&chain) {
            multistream.join(member.reader, member.methods);
            return;
        }
        let caches = self.caches_in(&mut state, chain);
        let multistream = Arc::new(
            Multistream::starting_with(chain, member.reader, member.methods)
                .with_planner(planner_for(chain, caches)),
        );
        lag::start_lag_tracking(Arc::clone(&multistream));
        state.multistreams.insert(chain, multistream);
    }

    /// The chain's caches, created on first use.
    pub fn caches(&self, chain: TargetBlockchain) -> Arc<Caches> {
        let mut state = self.state.write().unwrap();
        self.caches_in(&mut state, chain)
    }

    fn caches_in(&self, state: &mut State, chain: TargetBlockchain) -> Arc<Caches> {
        let caches = state.caches.entry(chain).or_insert_with(|| {
            let redis = self
                .redis
                .as_ref()
                .map(|conn| RedisCache::new(conn.clone(), chain.id()));
            Arc::new(Caches::with_redis(redis))
        });
        Arc::clone(caches)
    }

    /// Caches each new block the `head` reports, unless another upstream of
    /// the chain reported it already.
    pub fn cache_blocks_of(&self, chain: TargetBlockchain, head: &CurrentHead) {
        let caches = self.caches(chain);
        let mut state = self.state.write().unwrap();
        state
            .caching_heads
            .entry(chain)
            .or_insert_with(|| CachingHead::new(caches))
            .follow(head);
    }

    pub fn add_pending_source(&self, chain: TargetBlockchain, source: Arc<dyn PendingTxSource>) {
        let mut state = self.state.write().unwrap();
        state.add_pending_source(chain, source);
    }

    /// Relays the topics a remote Dshackle advertised, including its pending
    /// transactions if it has them.
    pub fn add_relay(&self, chain: TargetBlockchain, relay: Arc<DshackleEgress>) {
        let mut state = self.state.write().unwrap();
        if let Some(relayed) = RelayedPendingTxes::of(Arc::clone(&relay) as _) {
            state.add_pending_source(chain, Arc::new(relayed));
        }
        state.relays.entry(chain).or_default().push(relay);
    }

    /// Keeps the first one offered for the chain.
    pub fn offer_allowance_client(&self, chain: TargetBlockchain, client: &RemoteClient) {
        let mut state = self.state.write().unwrap();
        state
            .allowance_clients
            .entry(chain)
            .or_insert_with(|| client.clone());
    }

    /// Keeps the first one offered for the chain.
    pub fn offer_balance_client(&self, chain: TargetBlockchain, client: &RemoteClient) {
        let mut state = self.state.write().unwrap();
        state
            .balance_clients
            .entry(chain)
            .or_insert_with(|| client.clone());
    }

    /// All chains with at least one upstream, sorted by chain id for a
    /// deterministic `Describe` (legacy `MultistreamHolder.getAvailable`).
    pub fn list(&self) -> Vec<TargetBlockchain> {
        let state = self.state.read().unwrap();
        let mut chains: Vec<TargetBlockchain> = state.multistreams.keys().copied().collect();
        chains.sort_by_key(|c| c.id());
        chains
    }

    pub fn multistream(&self, chain: &TargetBlockchain) -> Option<Arc<Multistream>> {
        self.state.read().unwrap().multistreams.get(chain).cloned()
    }

    pub fn head(&self, chain: &TargetBlockchain) -> Option<Arc<MergedHead>> {
        self.state.read().unwrap().heads.get(chain).cloned()
    }

    pub fn existing_caches(&self, chain: &TargetBlockchain) -> Option<Arc<Caches>> {
        self.state.read().unwrap().caches.get(chain).cloned()
    }

    pub fn pending_txes(&self, chain: &TargetBlockchain) -> Option<Arc<dyn PendingTxSource>> {
        self.state.read().unwrap().pending_txes.get(chain).cloned()
    }

    pub fn relays(&self, chain: &TargetBlockchain) -> Vec<Arc<DshackleEgress>> {
        let state = self.state.read().unwrap();
        state.relays.get(chain).cloned().unwrap_or_default()
    }

    pub fn allowance_client(&self, chain: &TargetBlockchain) -> Option<RemoteClient> {
        self.state
            .read()
            .unwrap()
            .allowance_clients
            .get(chain)
            .cloned()
    }

    pub fn balance_client(&self, chain: &TargetBlockchain) -> Option<RemoteClient> {
        self.state
            .read()
            .unwrap()
            .balance_clients
            .get(chain)
            .cloned()
    }

    /// The current upstreams of each chain, for the periodic status log.
    pub fn statuses(&self) -> Vec<ChainStatus> {
        let state = self.state.read().unwrap();
        state
            .multistreams
            .iter()
            .map(|(chain, multistream)| ChainStatus {
                chain: *chain,
                upstreams: multistream.upstreams(),
            })
            .collect()
    }
}

impl State {
    /// Rebuilds the chain's aggregate with the new source. Whoever listens to
    /// the previous aggregate keeps it, without the new source.
    fn add_pending_source(&mut self, chain: TargetBlockchain, source: Arc<dyn PendingTxSource>) {
        let sources = self.pending_sources.entry(chain).or_default();
        sources.push(source);
        if let Some(combined) = AggregatedPendingTxes::combined(sources.clone()) {
            self.pending_txes.insert(chain, combined);
        }
    }
}

fn merge_order(chain: TargetBlockchain) -> MergeOrder {
    let TargetBlockchain::Standard(chain_ref) = chain;
    if is_pos(chain_ref) {
        MergeOrder::Priority
    } else {
        MergeOrder::Difficulty
    }
}
