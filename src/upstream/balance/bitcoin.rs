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

//! Native Bitcoin balance tracking. Ports the legacy `TrackBitcoinAddress`
//! `RpcUnspentReader` path: an address's balance is the sum of its unspent
//! outputs, read via `listunspent`.
//!
//! Only the `listunspent` RPC source is implemented. The legacy reader prefers
//! an Esplora indexer (and can delegate to a remote Dshackle that advertises
//! `BALANCE`); neither is ported yet, so balances resolve only for addresses the
//! node itself tracks (wallet/watch-only or an address-indexed node). xpub
//! address derivation is likewise deferred — callers get those addresses
//! rejected by `resolve_single_addresses`.

use super::{AddressReader, BalanceError, BalanceStream};
use crate::blockchain::TargetBlockchain;
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::bitcoin::btc_to_satoshis;
use crate::upstream::egress::ChainAccess;
use crate::upstream::merged_head::MergedHead;
use bitcoin::address::NetworkUnchecked;
use bitcoin::{Address, Network};
use emerald_api::proto::blockchain::{AddressBalance, Utxo, address_balance};
use emerald_api::proto::common::{Asset, ChainRef, SingleAddress};
use serde_json::{Value, json};
use std::str::FromStr;
use std::sync::Arc;

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
fn network_for(chain: TargetBlockchain) -> Network {
    match chain {
        TargetBlockchain::Standard(ChainRef::ChainBitcoin) => Network::Bitcoin,
        _ => Network::Testnet,
    }
}

/// One unspent output backing part of an address's balance.
struct Unspent {
    tx_id: String,
    index: u64,
    value: u64,
}

/// Native Bitcoin balance tracker for one chain.
pub struct BitcoinBalance {
    access: Arc<dyn ChainAccess>,
    head: Option<Arc<MergedHead>>,
    chain: i32,
    include_utxo: bool,
}

impl BitcoinBalance {
    pub fn new(
        access: Arc<dyn ChainAccess>,
        head: Option<Arc<MergedHead>>,
        chain: i32,
        include_utxo: bool,
    ) -> Self {
        Self {
            access,
            head,
            chain,
            include_utxo,
        }
    }

    /// One `AddressBalance` per address with its current balance. Backs
    /// `GetBalance`.
    pub fn get_balance(&self, addresses: Vec<String>) -> BalanceStream {
        super::one_shot(addresses, self.reader())
    }

    /// The current balance per address, then a fresh value whenever it changes
    /// on a new head. Backs `SubscribeBalance`.
    pub fn subscribe(&self, addresses: Vec<String>) -> BalanceStream {
        super::subscribe(self.head.clone(), addresses, self.reader())
    }

    fn reader(&self) -> AddressReader {
        let access = Arc::clone(&self.access);
        let chain = self.chain;
        let include_utxo = self.include_utxo;
        Arc::new(move |address| {
            let access = Arc::clone(&access);
            Box::pin(async move {
                let unspent = read_unspent(access.as_ref(), &address).await?;
                Ok(build_balance(chain, address, include_utxo, &unspent))
            })
        })
    }
}

/// Read an address's unspent outputs via `listunspent(1, 9999999, [address])`.
/// A failed call, an upstream error, or a missing result fails the read (legacy
/// `RpcUnspentReader` errors with `DataUnavailable`); an empty list is a valid
/// zero balance. Mirrors the legacy `RpcUnspentReader`.
async fn read_unspent(
    access: &dyn ChainAccess,
    address: &str,
) -> Result<Vec<Unspent>, tonic::Status> {
    let request = JsonRpcRequest::new(0, "listunspent".into(), json!([1, 9_999_999, [address]]));
    let response = access
        .call(&request)
        .await
        .map_err(|e| tonic::Status::unavailable(format!("balance read failed: {e}")))?;
    if let Some(error) = &response.error {
        return Err(tonic::Status::unavailable(format!(
            "upstream error reading unspent outputs: {}",
            error.message
        )));
    }
    let result = response
        .result
        .ok_or_else(|| tonic::Status::unavailable("empty listunspent response"))?;
    let items = serde_json::from_str::<Vec<Value>>(result.get())
        .map_err(|e| tonic::Status::internal(format!("invalid listunspent result: {e}")))?;
    Ok(items
        .iter()
        .filter(|u| u.get("address").and_then(Value::as_str) == Some(address))
        .filter_map(unspent)
        .collect())
}

/// Parse one `listunspent` entry, dropping any with missing/invalid fields.
fn unspent(item: &Value) -> Option<Unspent> {
    Some(Unspent {
        tx_id: item.get("txid")?.as_str()?.to_string(),
        index: item.get("vout")?.as_u64()?,
        value: btc_to_satoshis(item.get("amount")?)?,
    })
}

/// Sum the unspent outputs into an `AddressBalance`. The asset code is the
/// legacy constant `BTC` regardless of the request's `bitcoin`/`btc`/`satoshi`
/// spelling, and UTXOs are attached only when requested.
fn build_balance(
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
    use crate::upstream::traits::UpstreamError;
    use emerald_api::proto::blockchain::address_balance::BalanceType;
    use emerald_api::proto::common::ChainRef;
    use std::sync::Mutex;
    use tokio_stream::StreamExt;

    /// A `ChainAccess` returning a canned `listunspent` result (a JSON array).
    struct FakeChain {
        unspent: Mutex<Value>,
    }

    impl FakeChain {
        fn new(unspent: Value) -> Arc<Self> {
            Arc::new(Self {
                unspent: Mutex::new(unspent),
            })
        }
    }

    #[async_trait::async_trait]
    impl ChainAccess for FakeChain {
        fn is_syncing(&self) -> bool {
            false
        }
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            assert_eq!(request.method.as_str(), "listunspent");
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
        BitcoinBalance::new(chain, None, ChainRef::ChainBitcoin as i32, include_utxo)
    }

    async fn drain(mut stream: BalanceStream) -> Vec<AddressBalance> {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.unwrap());
        }
        out
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
