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

//! Native Bitcoin balance tracking. Ports the legacy `TrackBitcoinAddress`:
//! an address's balance is the sum of its unspent outputs.
//!
//! The outputs come from an [`UnspentSource`] chosen per chain — an Esplora
//! index when one is configured, the node's `listunspent` otherwise. The whole
//! path is gated on the operator's explicit `balance: true`, and requests to a
//! remote Dshackle advertising `BALANCE` are forwarded before reaching here.
//! Xpub requests run the scan in [`super::xpub_scan`] on the same source.

use super::unspent::{Unspent, UnspentSource};
use super::xpub_scan::UsedAddress;
use super::{BalanceError, BalanceStream};
use crate::blockchain::TargetBlockchain;
use crate::data::BlockContainer;
use crate::upstream::merged_head::MergedHead;
use bitcoin::address::NetworkUnchecked;
use bitcoin::{Address, Network};
use emerald_api::proto::blockchain::{AddressBalance, Utxo, address_balance};
use emerald_api::proto::common::{Asset, ChainRef, SingleAddress};
use futures::stream;
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// Validate Bitcoin addresses for `chain`'s network, erroring on the first
/// malformed or wrong-network address (legacy bitcoinj `Address.fromString`).
/// The addresses are returned unchanged — the legacy response echoes the input
/// verbatim, unlike Ethereum which normalizes.
pub fn validate_addresses(
    addresses: Vec<String>,
    chain: TargetBlockchain,
) -> Result<Vec<String>, BalanceError> {
    let network = network_for(chain);
    for address in &addresses {
        let parsed = Address::<NetworkUnchecked>::from_str(address)
            .map_err(|_| BalanceError::InvalidAddress(address.clone()))?;
        parsed
            .require_network(network)
            .map_err(|_| BalanceError::InvalidAddress(address.clone()))?;
    }
    Ok(addresses)
}

/// The Bitcoin network a chain runs on. Only the mainnet chain maps to
/// `Bitcoin`; the rest are Bitcoin testnets.
pub(super) fn network_for(chain: TargetBlockchain) -> Network {
    match chain {
        TargetBlockchain::Standard(ChainRef::ChainBitcoin) => Network::Bitcoin,
        _ => Network::Testnet,
    }
}

/// Native Bitcoin balance tracker for one chain.
pub struct BitcoinBalance {
    source: Arc<dyn UnspentSource>,
    head: Option<Arc<MergedHead>>,
    chain: i32,
    include_utxo: bool,
}

impl BitcoinBalance {
    /// A tracker reading unspent outputs from `source`. With a `head` a
    /// subscription re-reads on every new block; `include_utxo` attaches the
    /// individual outputs to each balance.
    pub fn new(
        source: Arc<dyn UnspentSource>,
        head: Option<Arc<MergedHead>>,
        chain: i32,
        include_utxo: bool,
    ) -> Self {
        Self {
            source,
            head,
            chain,
            include_utxo,
        }
    }

    /// One `AddressBalance` per address with its current balance. Backs
    /// `GetBalance`.
    pub fn get_balance(&self, addresses: Vec<String>) -> BalanceStream {
        self.watch(addresses, None, None)
    }

    /// The current balance per address, then a fresh value whenever it changes
    /// on a new head. Backs `SubscribeBalance`.
    pub fn subscribe(&self, addresses: Vec<String>) -> BalanceStream {
        self.watch(addresses, None, self.new_heads())
    }

    /// Balances of the addresses an xpub scan found, built straight from the
    /// scan's outputs — the scan already read them, so the source isn't asked
    /// twice. Backs `GetBalance` for xpub addresses.
    pub fn get_balance_scanned(&self, found: Vec<UsedAddress>) -> BalanceStream {
        let balances: Vec<_> = self.scanned_balances(found).into_iter().map(Ok).collect();
        Box::pin(stream::iter(balances))
    }

    /// Watch the set an xpub scan found: the scan's values first, then updates
    /// on new heads. The set is fixed at scan time — an address first used
    /// only later is not picked up, same as the legacy active-set subscribe.
    /// Backs `SubscribeBalance` for xpub addresses.
    pub fn subscribe_scanned(&self, found: Vec<UsedAddress>) -> BalanceStream {
        let addresses = found.iter().map(|used| used.address.clone()).collect();
        let current = self.scanned_balances(found);
        self.watch(addresses, Some(current), self.new_heads())
    }

    fn scanned_balances(&self, found: Vec<UsedAddress>) -> Vec<AddressBalance> {
        found
            .into_iter()
            .map(|used| build_balance(self.chain, used.address, self.include_utxo, &used.unspent))
            .collect()
    }

    fn new_heads(&self) -> Option<broadcast::Receiver<Arc<BlockContainer>>> {
        self.head.as_ref().map(|head| head.subscribe())
    }

    /// The balances of `addresses` as a stream. `current` are balances already
    /// known, sparing the first read; without `heads` the stream ends after
    /// the current values.
    fn watch(
        &self,
        addresses: Vec<String>,
        current: Option<Vec<AddressBalance>>,
        heads: Option<broadcast::Receiver<Arc<BlockContainer>>>,
    ) -> BalanceStream {
        let mut seen = HashSet::new();
        let queried = addresses
            .iter()
            .filter(|address| seen.insert(address.as_str()))
            .cloned()
            .collect();
        let watch = Watch {
            source: Arc::clone(&self.source),
            chain: self.chain,
            include_utxo: self.include_utxo,
            addresses,
            queried,
            heads,
            current,
            last: HashMap::new(),
            ready: VecDeque::new(),
            started: false,
            failed: false,
        };
        Box::pin(stream::unfold(watch, Watch::next))
    }
}

/// The balances of a fixed set of addresses as a stream: the current values
/// first, then, when following the chain head, the ones a new block changed
/// (legacy `distinctUntilChanged { balance }`).
///
/// Every round reads the whole set with a single call to the source, which
/// leaves it to the source how to spread the work: one batched `listunspent`,
/// or a bounded number of parallel requests to Esplora. Reading each address
/// on its own would hit the source with one request per address at once, on
/// every block, for as long as a wide subscription lives.
struct Watch {
    source: Arc<dyn UnspentSource>,
    chain: i32,
    include_utxo: bool,
    /// As requested: the response carries an entry per address in this order,
    /// repeated ones included.
    addresses: Vec<String>,
    /// `addresses` without the repeats, which Bitcoin Core rejects in a
    /// `listunspent` call.
    queried: Vec<String>,
    heads: Option<broadcast::Receiver<Arc<BlockContainer>>>,
    /// Current balances known before the first read.
    current: Option<Vec<AddressBalance>>,
    /// Last emitted balance per address.
    last: HashMap<String, String>,
    ready: VecDeque<AddressBalance>,
    started: bool,
    /// Set once a read errors: the error is emitted and ends the stream,
    /// rather than leaving a subscription that silently stopped updating.
    failed: bool,
}

impl Watch {
    async fn next(mut self) -> Option<(Result<AddressBalance, tonic::Status>, Self)> {
        loop {
            if let Some(balance) = self.ready.pop_front() {
                return Some((Ok(balance), self));
            }
            if self.failed {
                return None;
            }
            let first = !self.started;
            let round = if first {
                self.started = true;
                match self.current.take() {
                    Some(current) => Ok(current),
                    None => self.read().await,
                }
            } else {
                match self.heads.as_mut()?.recv().await {
                    Ok(_block) => self.read().await,
                    // A lagging subscriber skips the gap and re-reads on the next head.
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => return None,
                }
            };
            match round {
                Ok(balances) => {
                    for balance in balances {
                        let Some(address) = balance.address.as_ref().map(|a| &a.address) else {
                            continue;
                        };
                        let changed = self.last.get(address) != Some(&balance.balance);
                        if changed {
                            self.last.insert(address.clone(), balance.balance.clone());
                        }
                        // The current values answer the request as it was
                        // made, an entry per requested address.
                        if first || changed {
                            self.ready.push_back(balance);
                        }
                    }
                }
                Err(status) => {
                    self.failed = true;
                    return Some((Err(status), self));
                }
            }
        }
    }

    async fn read(&self) -> Result<Vec<AddressBalance>, tonic::Status> {
        let mut by_address: HashMap<String, Vec<Unspent>> = HashMap::new();
        for output in self.source.unspent(&self.queried).await? {
            by_address
                .entry(output.address.clone())
                .or_default()
                .push(output);
        }
        Ok(self
            .addresses
            .iter()
            .map(|address| {
                let unspent = by_address
                    .get(address)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                build_balance(self.chain, address.clone(), self.include_utxo, unspent)
            })
            .collect())
    }
}

/// Sum the unspent outputs (all belonging to `address`) into an
/// `AddressBalance`. The asset code is the legacy constant `BTC` regardless of
/// the request's `bitcoin`/`btc`/`satoshi` spelling, and UTXOs are attached
/// only when requested.
pub(super) fn build_balance(
    chain: i32,
    address: String,
    include_utxo: bool,
    unspent: &[Unspent],
) -> AddressBalance {
    let total: u64 = unspent.iter().map(|u| u.value).sum();
    let utxo = if include_utxo {
        unspent
            .iter()
            .map(|u| Utxo {
                tx_id: u.tx_id.clone(),
                index: u.index,
                balance: u.value.to_string(),
                spent: false,
            })
            .collect()
    } else {
        Vec::new()
    };
    AddressBalance {
        address: Some(SingleAddress { address }),
        balance: total.to_string(),
        confirmed: false,
        utxo,
        balance_type: Some(address_balance::BalanceType::Asset(Asset {
            chain,
            code: "BTC".to_string(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::balance::rpc_unspent::RpcUnspentSource;
    use crate::upstream::egress::ChainAccess;
    use crate::upstream::merged_head::MergeOrder;
    use crate::upstream::traits::UpstreamError;
    use emerald_api::proto::blockchain::address_balance::BalanceType;
    use emerald_api::proto::common::ChainRef;
    use serde_json::{Value, json};
    use std::sync::Mutex;
    use tokio_stream::StreamExt;

    /// A `ChainAccess` returning a canned `listunspent` result (a JSON array)
    /// and recording the addresses each call asked for.
    struct FakeChain {
        unspent: Mutex<Value>,
        calls: Mutex<Vec<Vec<String>>>,
    }

    impl FakeChain {
        fn new(unspent: Value) -> Arc<Self> {
            Arc::new(Self {
                unspent: Mutex::new(unspent),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<Vec<String>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl ChainAccess for FakeChain {
        fn is_syncing(&self) -> bool {
            false
        }
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            assert_eq!(request.method.as_str(), "listunspent");
            let addresses = serde_json::from_value(request.params[2].clone()).unwrap();
            self.calls.lock().unwrap().push(addresses);
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                serde_json::to_string(&*self.unspent.lock().unwrap()).unwrap()
            );
            Ok(serde_json::from_str(&body).unwrap())
        }
    }

    const ADDR: &str = "bc1qexampleaddr";

    fn utxo(txid: &str, vout: u64, amount: f64) -> Value {
        json!({ "txid": txid, "vout": vout, "address": ADDR, "amount": amount })
    }

    fn tracker(chain: Arc<FakeChain>, include_utxo: bool) -> BitcoinBalance {
        let source = Arc::new(RpcUnspentSource::new(chain));
        BitcoinBalance::new(source, None, ChainRef::ChainBitcoin as i32, include_utxo)
    }

    async fn drain(mut stream: BalanceStream) -> Vec<AddressBalance> {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.unwrap());
        }
        out
    }

    fn utxo_of(address: &str, amount: f64) -> Value {
        json!({ "txid": "aa", "vout": 0, "address": address, "amount": amount })
    }

    fn push_head(head: &MergedHead, height: u64) {
        use crate::data::BlockId;
        head.feed(
            0,
            Arc::new(BlockContainer {
                hash: BlockId::from_bytes([height as u8; 32]),
                height,
                parent_hash: None,
                total_difficulty: alloy::primitives::U256::ZERO,
                timestamp: jiff::Timestamp::UNIX_EPOCH,
                transaction_hashes: vec![],
                json: None,
                header_json: None,
            }),
        );
    }

    fn address_and_balance(balance: &AddressBalance) -> (&str, &str) {
        let address = balance.address.as_ref().unwrap().address.as_str();
        (address, balance.balance.as_str())
    }

    #[tokio::test]
    async fn reads_all_addresses_with_one_call() {
        let chain = FakeChain::new(json!([utxo_of("a", 0.5), utxo_of("b", 1.0)]));
        let tracker = tracker(Arc::clone(&chain), false);

        let out = drain(tracker.get_balance(vec!["b".into(), "a".into(), "b".into()])).await;

        let balances: Vec<_> = out.iter().map(address_and_balance).collect();
        assert_eq!(
            balances,
            [("b", "100000000"), ("a", "50000000"), ("b", "100000000")]
        );
        // The repeated address is answered twice but asked for once: Bitcoin
        // Core fails a `listunspent` naming the same address twice.
        assert_eq!(chain.calls(), [["b", "a"]]);
    }

    #[tokio::test]
    async fn subscription_reads_once_per_block_and_emits_what_changed() {
        let chain = FakeChain::new(json!([utxo_of("a", 0.5), utxo_of("b", 1.0)]));
        let head = Arc::new(MergedHead::new(MergeOrder::Priority));
        let source = Arc::new(RpcUnspentSource::new(
            Arc::clone(&chain) as Arc<dyn ChainAccess>
        ));
        let tracker = BitcoinBalance::new(
            source,
            Some(Arc::clone(&head)),
            ChainRef::ChainBitcoin as i32,
            false,
        );

        let mut stream = tracker.subscribe(vec!["a".into(), "b".into()]);
        let first = stream.next().await.unwrap().unwrap();
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(address_and_balance(&first), ("a", "50000000"));
        assert_eq!(address_and_balance(&second), ("b", "100000000"));

        *chain.unspent.lock().unwrap() = json!([utxo_of("a", 0.5), utxo_of("b", 3.0)]);
        push_head(&head, 1);

        let update = stream.next().await.unwrap().unwrap();
        assert_eq!(address_and_balance(&update), ("b", "300000000"));
        assert_eq!(chain.calls(), [["a", "b"], ["a", "b"]]);
    }

    #[tokio::test]
    async fn scanned_subscription_starts_from_the_scan_without_a_read() {
        let chain = FakeChain::new(json!([utxo_of("a", 2.0)]));
        let head = Arc::new(MergedHead::new(MergeOrder::Priority));
        let source = Arc::new(RpcUnspentSource::new(
            Arc::clone(&chain) as Arc<dyn ChainAccess>
        ));
        let tracker = BitcoinBalance::new(
            source,
            Some(Arc::clone(&head)),
            ChainRef::ChainBitcoin as i32,
            false,
        );
        let found = vec![UsedAddress {
            address: "a".into(),
            unspent: vec![Unspent {
                address: "a".into(),
                tx_id: "aa".into(),
                index: 0,
                value: 700,
            }],
        }];

        let mut stream = tracker.subscribe_scanned(found);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(address_and_balance(&first), ("a", "700"));
        assert!(chain.calls().is_empty());

        push_head(&head, 1);
        let update = stream.next().await.unwrap().unwrap();
        assert_eq!(address_and_balance(&update), ("a", "200000000"));
        assert_eq!(chain.calls(), [["a"]]);
    }

    #[tokio::test]
    async fn sums_unspent_outputs_into_balance() {
        // 0.5 + 1.25 BTC = 1.75 BTC = 175_000_000 sat.
        let chain = FakeChain::new(json!([utxo("aa", 0, 0.5), utxo("bb", 1, 1.25)]));
        let out = drain(tracker(chain, false).get_balance(vec![ADDR.into()])).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].balance, "175000000");
        assert!(out[0].utxo.is_empty());
        match out[0].balance_type.as_ref().unwrap() {
            BalanceType::Asset(a) => {
                assert_eq!(a.code, "BTC");
                assert_eq!(a.chain, ChainRef::ChainBitcoin as i32);
            }
            other => panic!("expected asset, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn include_utxo_attaches_each_output() {
        let chain = FakeChain::new(json!([utxo("aa", 0, 0.5), utxo("bb", 3, 1.25)]));
        let out = drain(tracker(chain, true).get_balance(vec![ADDR.into()])).await;
        assert_eq!(out[0].utxo.len(), 2);
        assert_eq!(out[0].utxo[0].tx_id, "aa");
        assert_eq!(out[0].utxo[0].index, 0);
        assert_eq!(out[0].utxo[0].balance, "50000000");
        assert!(!out[0].utxo[0].spent);
        assert_eq!(out[0].utxo[1].index, 3);
        assert_eq!(out[0].utxo[1].balance, "125000000");
    }

    #[tokio::test]
    async fn no_unspent_outputs_is_zero_balance() {
        let chain = FakeChain::new(json!([]));
        let out = drain(tracker(chain, true).get_balance(vec![ADDR.into()])).await;
        assert_eq!(out[0].balance, "0");
        assert!(out[0].utxo.is_empty());
    }

    #[test]
    fn validates_addresses_against_network() {
        let mainnet = TargetBlockchain::Standard(ChainRef::ChainBitcoin);
        // BIP173 example mainnet P2WPKH address.
        assert!(
            validate_addresses(
                vec!["bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4".into()],
                mainnet
            )
            .is_ok()
        );
        // Not an address at all.
        assert!(matches!(
            validate_addresses(vec!["not-an-address".into()], mainnet).unwrap_err(),
            BalanceError::InvalidAddress(s) if s == "not-an-address"
        ));
        // A testnet address rejected against the mainnet chain.
        assert!(
            validate_addresses(
                vec!["tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx".into()],
                mainnet
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn outputs_for_other_addresses_are_ignored() {
        let chain = FakeChain::new(json!([
            utxo("aa", 0, 0.5),
            json!({ "txid": "cc", "vout": 0, "address": "other", "amount": 9.0 }),
        ]));
        let out = drain(tracker(chain, false).get_balance(vec![ADDR.into()])).await;
        // Only the 0.5 BTC output for ADDR counts.
        assert_eq!(out[0].balance, "50000000");
    }
}
