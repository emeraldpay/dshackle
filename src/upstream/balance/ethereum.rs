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

//! Ethereum-family balance tracking: native Ether (legacy
//! `TrackEthereumAddress`) and ERC-20 tokens (legacy `TrackERC20Address`).
//!
//! Both read a balance per address (`eth_getBalance` vs an `eth_call` to the
//! token's `balanceOf`) and, on `subscribe`, emit the current value and then a
//! fresh one whenever it changes (legacy `distinctUntilChanged { balance }`).
//! They differ in *what wakes a re-read*:
//!
//! - **Native** re-reads on every new head: a coin balance can move without the
//!   address itself transacting (incoming transfers, block rewards), so there's
//!   no cheaper signal than the head.
//! - **ERC-20** re-reads only when a `Transfer` log for the token touches the
//!   address in a new block, matching legacy `TrackERC20Address`. Polling
//!   `balanceOf` every block would multiply upstream `eth_call` load by the
//!   block rate while almost always reading an unchanged value.
//!
//! A single [`BalanceKind`] carries which balance to read and how to echo the
//! asset; the subscription path forks on it.

use super::{AddressReader, BalanceStream, native_balance, parse_hex_u256};
use crate::data::{BlockContainer, BlockId};
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::egress::ChainAccess;
use crate::upstream::merged_head::MergedHead;
use alloy::primitives::{Address, B256, U256};
use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};
use emerald_api::proto::blockchain::{AddressBalance, address_balance};
use emerald_api::proto::common::{Asset, Erc20Asset, SingleAddress};
use serde_json::{Value, json};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

sol! {
    interface IERC20 {
        function balanceOf(address owner) external view returns (uint256);
        event Transfer(address indexed from, address indexed to, uint256 value);
    }
}

/// What balance to read and how to echo the asset in the response.
#[derive(Clone)]
enum BalanceKind {
    /// Native coin via `eth_getBalance`, echoed as the request's `Asset`.
    Native(Asset),
    /// ERC-20 token via an `eth_call` to `contract`'s `balanceOf`, echoed per
    /// [`Erc20Echo`].
    Erc20 { contract: String, echo: Erc20Echo },
}

/// How an ERC-20 response identifies the token — matching how the request asked
/// for it (legacy `TrackERC20Address.buildResponse`).
#[derive(Clone)]
enum Erc20Echo {
    /// Requested by configured code → `Asset { code: NAME.uppercase() }`.
    Named { chain: i32, name: String },
    /// Requested by contract → `Erc20Asset { contract_address }`.
    Contract { chain: i32, contract: String },
}

/// Ethereum-family balance tracker for one chain. Holds the chain's call
/// surface, its merged head (for subscriptions), and what to read.
pub struct EthereumBalance {
    access: Arc<dyn ChainAccess>,
    head: Option<Arc<MergedHead>>,
    kind: BalanceKind,
}

impl EthereumBalance {
    /// Native Ether tracker echoing `asset` (the request's `ether` asset).
    pub fn native(access: Arc<dyn ChainAccess>, head: Option<Arc<MergedHead>>, asset: Asset) -> Self {
        Self {
            access,
            head,
            kind: BalanceKind::Native(asset),
        }
    }

    /// ERC-20 tracker for a token requested by its configured code; the response
    /// echoes the code (uppercased) as an `Asset`.
    pub fn erc20_named(
        access: Arc<dyn ChainAccess>,
        head: Option<Arc<MergedHead>>,
        chain: i32,
        name: String,
        contract: String,
    ) -> Self {
        Self {
            access,
            head,
            kind: BalanceKind::Erc20 {
                contract,
                echo: Erc20Echo::Named { chain, name },
            },
        }
    }

    /// ERC-20 tracker for a token requested by contract address; the response
    /// echoes the contract as an `Erc20Asset`.
    pub fn erc20_contract(
        access: Arc<dyn ChainAccess>,
        head: Option<Arc<MergedHead>>,
        chain: i32,
        contract: String,
    ) -> Self {
        Self {
            access,
            head,
            kind: BalanceKind::Erc20 {
                contract: contract.clone(),
                echo: Erc20Echo::Contract { chain, contract },
            },
        }
    }

    /// One `AddressBalance` per address with the current balance. Backs
    /// `GetBalance`.
    pub fn get_balance(&self, addresses: Vec<String>) -> BalanceStream {
        super::one_shot(addresses, self.reader())
    }

    /// The current balance per address, then a fresh value whenever it changes.
    /// Native re-reads on every head; ERC-20 re-reads only when a `Transfer` log
    /// touches the address. Backs `SubscribeBalance`.
    pub fn subscribe(&self, addresses: Vec<String>) -> BalanceStream {
        match &self.kind {
            BalanceKind::Native(_) => {
                super::subscribe(self.head.clone(), addresses, self.reader())
            }
            BalanceKind::Erc20 { contract, .. } => erc20_subscribe(
                self.head.clone(),
                Arc::clone(&self.access),
                contract.clone(),
                addresses,
                self.reader(),
            ),
        }
    }

    /// A reader closure that captures this tracker's call surface and what to
    /// read, for the shared streaming helpers.
    fn reader(&self) -> AddressReader {
        let access = Arc::clone(&self.access);
        let kind = self.kind.clone();
        Arc::new(move |address| {
            let access = Arc::clone(&access);
            let kind = kind.clone();
            Box::pin(async move {
                let balance = read_balance(access.as_ref(), &kind, &address).await?;
                Ok(respond(&kind, address, balance))
            })
        })
    }
}

/// Read an address's balance for the given [`BalanceKind`], pinned to the chain
/// head (legacy reads at `head.getCurrentHeight()`). A failed call, an upstream
/// error, or a missing result fails the read so the stream errors rather than
/// reporting a fabricated zero.
async fn read_balance(
    access: &dyn ChainAccess,
    kind: &BalanceKind,
    address: &str,
) -> Result<U256, tonic::Status> {
    let block = block_param(access);
    let request = match kind {
        BalanceKind::Native(_) => {
            JsonRpcRequest::new(0, "eth_getBalance".into(), json!([address, block]))
        }
        BalanceKind::Erc20 { contract, .. } => {
            // Addresses are validated before reaching the tracker, so this
            // parse succeeds; guard it anyway rather than panic.
            let owner = address.parse::<Address>().map_err(|_| {
                tonic::Status::invalid_argument(format!("invalid address: {address}"))
            })?;
            let data = alloy::hex::encode_prefixed(IERC20::balanceOfCall { owner }.abi_encode());
            let call = json!({ "to": contract, "data": data });
            JsonRpcRequest::new(0, "eth_call".into(), json!([call, block]))
        }
    };

    let response = access
        .call(&request)
        .await
        .map_err(|e| tonic::Status::unavailable(format!("balance read failed: {e}")))?;
    if let Some(error) = &response.error {
        return Err(tonic::Status::unavailable(format!(
            "upstream error reading balance: {}",
            error.message
        )));
    }
    let result = response
        .result
        .ok_or_else(|| tonic::Status::unavailable("empty balance response"))?;
    let hex = serde_json::from_str::<String>(result.get())
        .map_err(|e| tonic::Status::internal(format!("invalid balance result: {e}")))?;
    Ok(decode_balance(kind, &hex))
}

/// The block tag for a balance read: the chain's current head height as hex, or
/// `"latest"` when no head is known yet (legacy `head.getCurrentHeight()` with a
/// `"latest"` fallback).
fn block_param(access: &dyn ChainAccess) -> serde_json::Value {
    match access.current_height() {
        Some(height) => json!(format!("0x{height:x}")),
        None => json!("latest"),
    }
}

/// Decode a balance read's hex result. `eth_getBalance` returns a plain hex
/// quantity; `eth_call` returns the ABI-encoded `balanceOf` return, decoded via
/// the generated decoder. Either way a malformed result yields zero.
fn decode_balance(kind: &BalanceKind, hex: &str) -> U256 {
    match kind {
        BalanceKind::Native(_) => parse_hex_u256(hex),
        BalanceKind::Erc20 { .. } => match alloy::hex::decode(hex) {
            Ok(bytes) => IERC20::balanceOfCall::abi_decode_returns(&bytes).unwrap_or(U256::ZERO),
            Err(_) => U256::ZERO,
        },
    }
}

/// Build the `AddressBalance` response for a read.
fn respond(kind: &BalanceKind, address: String, balance: U256) -> AddressBalance {
    match kind {
        BalanceKind::Native(asset) => native_balance(asset.clone(), address, balance),
        BalanceKind::Erc20 { echo, .. } => {
            let balance_type = match echo {
                Erc20Echo::Named { chain, name } => {
                    address_balance::BalanceType::Asset(Asset {
                        chain: *chain,
                        code: name.to_uppercase(),
                    })
                }
                Erc20Echo::Contract { chain, contract } => {
                    address_balance::BalanceType::Erc20Asset(Erc20Asset {
                        chain: *chain,
                        contract_address: contract.clone(),
                    })
                }
            };
            AddressBalance {
                address: Some(SingleAddress { address }),
                balance: balance.to_string(),
                confirmed: false,
                utxo: Vec::new(),
                balance_type: Some(balance_type),
            }
        }
    }
}

/// One tracked owner of an ERC-20 token: the address string used for reads and
/// echoed in the response, its 20-byte form for matching `Transfer` topics, and
/// the last balance emitted (for `distinctUntilChanged`).
struct Owner {
    address: String,
    /// `None` when the address didn't parse; such an owner never matches a log,
    /// and its initial read fails the stream through the reader, as before.
    word: Option<Address>,
    last: Option<String>,
}

/// Whether the token was active for the tracked owners in a block.
enum Activity {
    /// Logs were read: exactly these owners were touched (empty = none).
    Touched(HashSet<Address>),
    /// Logs couldn't be read; re-read every owner rather than risk going stale.
    Unknown,
}

/// ERC-20 `SubscribeBalance`: emit each address's current balance, then re-read
/// and re-emit only when a `Transfer` log for the token touches that address in
/// a new block. Ports legacy `TrackERC20Address` (log-driven, not head-polled).
///
/// One `eth_getLogs` per block — scoped by block hash and filtered to the
/// contract's `Transfer` events — is shared across all requested addresses; the
/// `from`/`to` match is applied per owner in code, since a single filter can't
/// express "owner as sender OR recipient".
fn erc20_subscribe(
    head: Option<Arc<MergedHead>>,
    access: Arc<dyn ChainAccess>,
    contract: String,
    addresses: Vec<String>,
    reader: AddressReader,
) -> BalanceStream {
    let Some(head) = head else {
        return super::one_shot(addresses, reader);
    };

    let owners: Vec<Owner> = addresses
        .into_iter()
        .map(|address| Owner {
            word: address.parse::<Address>().ok(),
            address,
            last: None,
        })
        .collect();

    struct State {
        head: broadcast::Receiver<Arc<BlockContainer>>,
        access: Arc<dyn ChainAccess>,
        contract: String,
        reader: AddressReader,
        owners: Vec<Owner>,
        /// Next owner still owed its initial balance emission.
        init_next: usize,
        pending: VecDeque<Result<AddressBalance, tonic::Status>>,
        /// Set once a read errors; the next poll ends the stream after the error
        /// (a balance read failing should fail the subscription).
        done: bool,
    }

    let state = State {
        head: head.subscribe(),
        access,
        contract,
        reader,
        owners,
        init_next: 0,
        pending: VecDeque::new(),
        done: false,
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        loop {
            if state.done {
                return None;
            }
            if let Some(item) = state.pending.pop_front() {
                state.done = item.is_err();
                return Some((item, state));
            }
            // Emit the current balance of every address before reacting to logs.
            if state.init_next < state.owners.len() {
                let index = state.init_next;
                state.init_next += 1;
                let address = state.owners[index].address.clone();
                return match (state.reader)(address).await {
                    Ok(balance) => {
                        state.owners[index].last = Some(balance.balance.clone());
                        Some((Ok(balance), state))
                    }
                    Err(status) => {
                        state.done = true;
                        Some((Err(status), state))
                    }
                };
            }
            match state.head.recv().await {
                Ok(block) => {
                    let activity =
                        token_activity(&*state.access, &block.hash, &state.contract, &state.owners)
                            .await;
                    for index in 0..state.owners.len() {
                        let touched = match &activity {
                            Activity::Unknown => true,
                            Activity::Touched(set) => {
                                state.owners[index].word.is_some_and(|w| set.contains(&w))
                            }
                        };
                        if !touched {
                            continue;
                        }
                        let address = state.owners[index].address.clone();
                        match (state.reader)(address).await {
                            Ok(balance)
                                if state.owners[index].last.as_deref()
                                    != Some(balance.balance.as_str()) =>
                            {
                                state.owners[index].last = Some(balance.balance.clone());
                                state.pending.push_back(Ok(balance));
                            }
                            // Unchanged — suppressed, like legacy distinctUntilChanged.
                            Ok(_) => {}
                            Err(status) => {
                                state.pending.push_back(Err(status));
                                break;
                            }
                        }
                    }
                }
                // A lagging subscriber skips the gap and reacts to the next head.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Box::pin(stream)
}

/// Read the contract's `Transfer` logs for one block and report which tracked
/// owners appear as sender or recipient. A failed call, JSON-RPC error, or
/// unparseable result yields [`Activity::Unknown`] so the caller re-reads rather
/// than missing a balance change.
async fn token_activity(
    access: &dyn ChainAccess,
    block_hash: &BlockId,
    contract: &str,
    owners: &[Owner],
) -> Activity {
    let filter = json!({
        "blockHash": block_hash.to_hex_prefixed(),
        "address": contract,
        "topics": [format!("{:#x}", IERC20::Transfer::SIGNATURE_HASH)],
    });
    let request = JsonRpcRequest::new(0, "eth_getLogs".into(), json!([filter]));
    let Ok(response) = access.call(&request).await else {
        return Activity::Unknown;
    };
    if response.error.is_some() {
        return Activity::Unknown;
    }
    let Some(result) = response.result else {
        return Activity::Unknown;
    };
    let Ok(logs) = serde_json::from_str::<Vec<Value>>(result.get()) else {
        return Activity::Unknown;
    };

    let wanted: HashSet<Address> = owners.iter().filter_map(|o| o.word).collect();
    let mut touched = HashSet::new();
    for log in &logs {
        let Some(topics) = log.get("topics").and_then(Value::as_array) else {
            continue;
        };
        // topics[1] = from, topics[2] = to (the indexed Transfer args).
        for topic in topics.iter().skip(1).take(2) {
            if let Some(address) = topic_address(topic)
                && wanted.contains(&address)
            {
                touched.insert(address);
            }
        }
    }
    Activity::Touched(touched)
}

/// Decode a 32-byte log topic as the address in its low 20 bytes, or `None` if
/// it isn't a valid word.
fn topic_address(topic: &Value) -> Option<Address> {
    let word = topic.as_str()?.parse::<B256>().ok()?;
    Some(Address::from_word(word))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::traits::UpstreamError;
    use emerald_api::proto::blockchain::address_balance::BalanceType;
    use emerald_api::proto::common::ChainRef;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio_stream::StreamExt;

    /// A `ChainAccess` serving canned hex results: `eth_getBalance` keyed by the
    /// queried address, `eth_call` keyed by the `to` contract.
    struct FakeChain {
        results: Mutex<HashMap<String, String>>,
    }

    impl FakeChain {
        fn new(entries: &[(&str, &str)]) -> Arc<Self> {
            Arc::new(Self {
                results: Mutex::new(
                    entries
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                ),
            })
        }
    }

    #[async_trait::async_trait]
    impl ChainAccess for FakeChain {
        fn is_syncing(&self) -> bool {
            false
        }
        async fn call(
            &self,
            request: &JsonRpcRequest,
        ) -> Result<JsonRpcResponse, UpstreamError> {
            let key = match request.method.as_str() {
                "eth_getBalance" => request.params[0].as_str().unwrap().to_string(),
                "eth_call" => request.params[0]["to"].as_str().unwrap().to_string(),
                other => panic!("unexpected method {other}"),
            };
            let hex = self
                .results
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_else(|| "0x0".to_string());
            let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{hex}"}}"#);
            Ok(serde_json::from_str(&body).unwrap())
        }
    }

    fn ether_asset() -> Asset {
        Asset {
            chain: ChainRef::ChainEthereum as i32,
            code: "ether".to_string(),
        }
    }

    async fn drain(mut stream: BalanceStream) -> Vec<AddressBalance> {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.unwrap());
        }
        out
    }

    // ── native Ether ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn native_get_balance_reads_single_address() {
        // 0x72fa5e0181 == 493826736513 wei.
        let chain = FakeChain::new(&[("0xabc", "0x72fa5e0181")]);
        let tracker = EthereumBalance::native(chain, None, ether_asset());
        let out = drain(tracker.get_balance(vec!["0xabc".into()])).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].balance, "493826736513");
        assert_eq!(out[0].address.as_ref().unwrap().address, "0xabc");
        assert!(matches!(out[0].balance_type, Some(BalanceType::Asset(_))));
    }

    #[tokio::test]
    async fn native_reports_zero_for_unknown_address() {
        let chain = FakeChain::new(&[]);
        let tracker = EthereumBalance::native(chain, None, ether_asset());
        let out = drain(tracker.get_balance(vec!["0xdead".into()])).await;
        assert_eq!(out[0].balance, "0");
    }

    #[tokio::test]
    async fn native_reads_each_address() {
        let chain = FakeChain::new(&[("0xa", "0x1"), ("0xb", "0xff")]);
        let tracker = EthereumBalance::native(chain, None, ether_asset());
        let out = drain(tracker.get_balance(vec!["0xa".into(), "0xb".into()])).await;
        let balances: Vec<&str> = out.iter().map(|b| b.balance.as_str()).collect();
        assert_eq!(balances, vec!["1", "255"]);
    }

    #[tokio::test]
    async fn native_subscribe_without_head_emits_current_once() {
        let chain = FakeChain::new(&[("0xabc", "0x64")]); // 100
        let tracker = EthereumBalance::native(chain, None, ether_asset());
        let out = drain(tracker.subscribe(vec!["0xabc".into()])).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].balance, "100");
    }

    #[tokio::test]
    async fn native_subscribe_emits_initial_then_update_on_head() {
        use crate::data::BlockContainer;
        use crate::upstream::head::CurrentHead;

        let chain = FakeChain::new(&[("0xabc", "0x10")]); // 16
        let current = Arc::new(CurrentHead::new());
        let head = MergedHead::new(vec![Arc::clone(&current)]);
        let tracker = EthereumBalance::native(chain.clone(), Some(head), ether_asset());

        let mut stream = tracker.subscribe(vec!["0xabc".into()]);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.balance, "16");

        chain
            .results
            .lock()
            .unwrap()
            .insert("0xabc".into(), "0x20".into()); // 32
        current.update_with_block(BlockContainer {
            hash: crate::data::BlockId::from_bytes([1u8; 32]),
            height: 1,
            parent_hash: None,
            total_difficulty: U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        });

        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.balance, "32");
    }

    // ── ERC-20 ───────────────────────────────────────────────────────────

    const CONTRACT: &str = "0x6b175474e89094c44da98b954eedeac495271d0f"; // DAI-like
    const OWNER: &str = "0x000000000000000000000000000000000000dead";

    /// A `uint256` ABI-encoded as the 32-byte return a node sends for an
    /// `eth_call` to `balanceOf`.
    fn abi_uint(value: u64) -> String {
        format!("0x{value:064x}")
    }

    #[tokio::test]
    async fn erc20_by_contract_calls_balance_of_and_echoes_contract() {
        let chain = FakeChain::new(&[(CONTRACT, &abi_uint(1000))]);
        let tracker = EthereumBalance::erc20_contract(
            chain,
            None,
            ChainRef::ChainEthereum as i32,
            CONTRACT.to_string(),
        );
        let out = drain(tracker.get_balance(vec![OWNER.into()])).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].balance, "1000");
        match out[0].balance_type.as_ref().unwrap() {
            BalanceType::Erc20Asset(e) => assert_eq!(e.contract_address, CONTRACT),
            other => panic!("expected erc20 asset, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn erc20_by_name_echoes_uppercased_code_as_asset() {
        let chain = FakeChain::new(&[(CONTRACT, &abi_uint(42))]);
        let tracker = EthereumBalance::erc20_named(
            chain,
            None,
            ChainRef::ChainEthereum as i32,
            "dai".to_string(),
            CONTRACT.to_string(),
        );
        let out = drain(tracker.get_balance(vec![OWNER.into()])).await;
        assert_eq!(out[0].balance, "42");
        match out[0].balance_type.as_ref().unwrap() {
            BalanceType::Asset(a) => assert_eq!(a.code, "DAI"),
            other => panic!("expected asset, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn erc20_unparsable_owner_errors() {
        // The dispatch validates addresses, but the tracker still rejects a
        // non-20-byte owner rather than issuing a malformed call.
        let chain = FakeChain::new(&[(CONTRACT, &abi_uint(999))]);
        let tracker = EthereumBalance::erc20_contract(
            chain,
            None,
            ChainRef::ChainEthereum as i32,
            CONTRACT.to_string(),
        );
        let mut stream = tracker.get_balance(vec!["0xabc".into()]);
        assert!(stream.next().await.unwrap().is_err());
    }

    #[tokio::test]
    async fn read_failure_errors_the_stream() {
        // A transient upstream failure must fail the stream, not report "0".
        struct ErrorChain;
        #[async_trait::async_trait]
        impl ChainAccess for ErrorChain {
            fn is_syncing(&self) -> bool {
                false
            }
            async fn call(
                &self,
                _: &JsonRpcRequest,
            ) -> Result<JsonRpcResponse, UpstreamError> {
                Err(UpstreamError::Transport("upstream down".into()))
            }
        }
        let tracker = EthereumBalance::native(Arc::new(ErrorChain), None, ether_asset());
        let mut stream = tracker.get_balance(vec!["0xabc".into()]);
        let item = stream.next().await.unwrap();
        assert_eq!(item.unwrap_err().code(), tonic::Code::Unavailable);
    }

    #[test]
    fn balance_of_call_uses_the_canonical_selector() {
        let owner: Address = OWNER.parse().unwrap();
        let data = IERC20::balanceOfCall { owner }.abi_encode();
        // keccak256("balanceOf(address)")[..4] = 0x70a08231, then the padded arg.
        assert_eq!(&data[..4], &[0x70, 0xa0, 0x82, 0x31]);
        assert_eq!(data.len(), 4 + 32);
    }

    // ── ERC-20 log-driven subscription ───────────────────────────────────

    const OTHER: &str = "0x0000000000000000000000000000000000000001";

    /// A `ChainAccess` for the subscription path: `eth_call` returns the next
    /// `balanceOf` from a queue (counting invocations, so a test can prove quiet
    /// blocks don't read), and `eth_getLogs` returns canned logs keyed by the
    /// block hash. The queue is consumed per read, repeating its last value — so
    /// a test can give distinct values to successive reads within one poll, which
    /// `set_balance` (a single repeating value) can't express.
    struct LogChain {
        responses: Mutex<VecDeque<String>>,
        logs_by_block: Mutex<HashMap<String, Vec<serde_json::Value>>>,
        calls: Mutex<u32>,
        fail_logs: Mutex<bool>,
    }

    impl LogChain {
        fn new(balance: &str) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(VecDeque::from([balance.to_string()])),
                logs_by_block: Mutex::new(HashMap::new()),
                calls: Mutex::new(0),
                fail_logs: Mutex::new(false),
            })
        }
        fn set_balance(&self, hex: &str) {
            *self.responses.lock().unwrap() = VecDeque::from([hex.to_string()]);
        }
        fn set_balances(&self, hexes: &[String]) {
            *self.responses.lock().unwrap() = hexes.iter().cloned().collect();
        }
        fn set_logs(&self, block: [u8; 32], logs: Vec<serde_json::Value>) {
            let hash = BlockId::from_bytes(block).to_hex_prefixed();
            self.logs_by_block.lock().unwrap().insert(hash, logs);
        }
        fn calls(&self) -> u32 {
            *self.calls.lock().unwrap()
        }
    }

    #[async_trait::async_trait]
    impl ChainAccess for LogChain {
        fn is_syncing(&self) -> bool {
            false
        }
        fn current_height(&self) -> Option<u64> {
            Some(1)
        }
        async fn call(
            &self,
            request: &JsonRpcRequest,
        ) -> Result<JsonRpcResponse, UpstreamError> {
            let body = match request.method.as_str() {
                "eth_call" => {
                    *self.calls.lock().unwrap() += 1;
                    let mut queue = self.responses.lock().unwrap();
                    // Consume one per read; keep repeating the last once drained.
                    let hex = if queue.len() > 1 {
                        queue.pop_front().unwrap()
                    } else {
                        queue.front().cloned().unwrap()
                    };
                    format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{hex}"}}"#)
                }
                "eth_getLogs" => {
                    if *self.fail_logs.lock().unwrap() {
                        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}"#
                            .to_string()
                    } else {
                        let hash = request.params[0]["blockHash"].as_str().unwrap();
                        let logs = self
                            .logs_by_block
                            .lock()
                            .unwrap()
                            .get(hash)
                            .cloned()
                            .unwrap_or_default();
                        format!(
                            r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                            serde_json::to_string(&logs).unwrap()
                        )
                    }
                }
                other => panic!("unexpected method {other}"),
            };
            Ok(serde_json::from_str(&body).unwrap())
        }
    }

    /// A 32-byte topic carrying `addr` in its low 20 bytes, as a node encodes an
    /// indexed address argument.
    fn addr_topic(addr: &str) -> String {
        format!("0x{:0>64}", addr.trim_start_matches("0x"))
    }

    /// A `Transfer(from, to, value)` log as returned by `eth_getLogs`.
    fn transfer_log(from: &str, to: &str) -> serde_json::Value {
        json!({
            "topics": [
                format!("{:#x}", IERC20::Transfer::SIGNATURE_HASH),
                addr_topic(from),
                addr_topic(to),
            ],
        })
    }

    fn push_head(current: &crate::upstream::head::CurrentHead, block: [u8; 32], height: u64) {
        current.update_with_block(BlockContainer {
            hash: BlockId::from_bytes(block),
            height,
            parent_hash: None,
            total_difficulty: U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        });
    }

    #[tokio::test]
    async fn erc20_subscribe_skips_quiet_blocks_and_reads_on_transfer() {
        use crate::upstream::head::CurrentHead;
        let chain = LogChain::new(&abi_uint(100));
        let current = Arc::new(CurrentHead::new());
        let head = MergedHead::new(vec![Arc::clone(&current)]);
        let tracker = EthereumBalance::erc20_contract(
            chain.clone(),
            Some(head),
            ChainRef::ChainEthereum as i32,
            CONTRACT.to_string(),
        );

        let mut stream = tracker.subscribe(vec![OWNER.into()]);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.balance, "100");
        assert_eq!(chain.calls(), 1, "initial read only");

        // The balance moves, but the next block carries no Transfer for OWNER, so
        // it must NOT read — the whole point of the fix.
        chain.set_balance(&abi_uint(200));
        push_head(&current, [1u8; 32], 1);
        // A later block transfers the token TO OWNER: now a re-read is due.
        chain.set_logs([2u8; 32], vec![transfer_log(OTHER, OWNER)]);
        push_head(&current, [2u8; 32], 2);

        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.balance, "200");
        assert_eq!(
            chain.calls(),
            2,
            "one read for the touching block; the quiet block was skipped"
        );
    }

    #[tokio::test]
    async fn erc20_subscribe_matches_owner_as_sender() {
        use crate::upstream::head::CurrentHead;
        let chain = LogChain::new(&abi_uint(100));
        let current = Arc::new(CurrentHead::new());
        let head = MergedHead::new(vec![Arc::clone(&current)]);
        let tracker = EthereumBalance::erc20_contract(
            chain.clone(),
            Some(head),
            ChainRef::ChainEthereum as i32,
            CONTRACT.to_string(),
        );

        let mut stream = tracker.subscribe(vec![OWNER.into()]);
        assert_eq!(stream.next().await.unwrap().unwrap().balance, "100");

        // OWNER is the `from` of the Transfer (topic1).
        chain.set_balance(&abi_uint(40));
        chain.set_logs([1u8; 32], vec![transfer_log(OWNER, OTHER)]);
        push_head(&current, [1u8; 32], 1);

        assert_eq!(stream.next().await.unwrap().unwrap().balance, "40");
    }

    #[tokio::test]
    async fn erc20_subscribe_suppresses_unchanged_balance() {
        use crate::upstream::head::CurrentHead;
        let chain = LogChain::new(&abi_uint(100));
        let current = Arc::new(CurrentHead::new());
        let head = MergedHead::new(vec![Arc::clone(&current)]);
        let tracker = EthereumBalance::erc20_contract(
            chain.clone(),
            Some(head),
            ChainRef::ChainEthereum as i32,
            CONTRACT.to_string(),
        );

        let mut stream = tracker.subscribe(vec![OWNER.into()]);
        assert_eq!(stream.next().await.unwrap().unwrap().balance, "100");

        // Both blocks below touch OWNER and are processed within one poll, so the
        // reads' values come from the queue: the first re-read is unchanged (100,
        // suppressed), the second changes (300, emitted).
        chain.set_balances(&[abi_uint(100), abi_uint(300)]);
        chain.set_logs([1u8; 32], vec![transfer_log(OTHER, OWNER)]);
        push_head(&current, [1u8; 32], 1);
        chain.set_logs([2u8; 32], vec![transfer_log(OTHER, OWNER)]);
        push_head(&current, [2u8; 32], 2);

        assert_eq!(stream.next().await.unwrap().unwrap().balance, "300");
        assert_eq!(
            chain.calls(),
            3,
            "read on both touching blocks; the unchanged one was suppressed"
        );
    }

    #[tokio::test]
    async fn erc20_subscribe_reads_when_logs_unavailable() {
        // If the logs can't be read, fall back to reading rather than going stale.
        use crate::upstream::head::CurrentHead;
        let chain = LogChain::new(&abi_uint(100));
        *chain.fail_logs.lock().unwrap() = true;
        let current = Arc::new(CurrentHead::new());
        let head = MergedHead::new(vec![Arc::clone(&current)]);
        let tracker = EthereumBalance::erc20_contract(
            chain.clone(),
            Some(head),
            ChainRef::ChainEthereum as i32,
            CONTRACT.to_string(),
        );

        let mut stream = tracker.subscribe(vec![OWNER.into()]);
        assert_eq!(stream.next().await.unwrap().unwrap().balance, "100");

        chain.set_balance(&abi_uint(250));
        push_head(&current, [1u8; 32], 1);
        assert_eq!(stream.next().await.unwrap().unwrap().balance, "250");
    }
}
