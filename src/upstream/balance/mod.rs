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

//! Address balance tracking for the gRPC `GetBalance` / `SubscribeBalance`
//! methods.
//!
//! A request names an asset (which carries the chain) and an address (single,
//! multi, or — for Bitcoin — an xpub). Both methods stream one `AddressBalance`
//! per resolved single address; `SubscribeBalance` additionally re-emits when a
//! balance changes. The right tracker is chosen by chain + asset, mirroring the
//! legacy `trackAddress.find { it.isSupported(request) }` dispatch.
//!
//! Implemented so far: native Ether (see [`ethereum`]). ERC-20 and Bitcoin
//! (single/multi/xpub + UTXO) land in later slices.

pub mod bitcoin;
pub mod ethereum;

pub use bitcoin::BitcoinBalance;
pub use ethereum::EthereumBalance;

use crate::blockchain::TargetBlockchain;
use crate::data::BlockContainer;
use crate::upstream::merged_head::MergedHead;
use alloy::primitives::U256;
use emerald_api::proto::blockchain::{AddressBalance, BalanceRequest, address_balance};
use emerald_api::proto::common::any_address::AddrType;
use emerald_api::proto::common::{AnyAddress, Asset, SingleAddress};
use futures::future::BoxFuture;
use futures::stream::{self, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio_stream::Stream;

/// A stream of `AddressBalance` responses, matching the gRPC handler's balance
/// stream type. Errors are carried as `tonic::Status` so a failing stream can
/// surface to the client.
pub type BalanceStream = Pin<Box<dyn Stream<Item = Result<AddressBalance, tonic::Status>> + Send>>;

/// Why a balance request can't be served.
#[derive(Debug, PartialEq, Eq)]
pub enum BalanceError {
    /// The request set neither `asset` nor `erc20_asset`.
    NoAsset,
    /// The request carried no address.
    NoAddress,
    /// An address failed validation (bad format or wrong network).
    InvalidAddress(String),
    /// No upstream is configured for the requested chain.
    Unavailable(i32),
    /// No tracker supports this chain + asset combination yet.
    Unsupported,
}

impl BalanceError {
    /// Map to the gRPC status the handler returns.
    pub fn into_status(self) -> tonic::Status {
        match self {
            BalanceError::NoAsset => {
                tonic::Status::invalid_argument("neither asset nor erc20_asset is specified")
            }
            BalanceError::NoAddress => tonic::Status::invalid_argument("no address specified"),
            BalanceError::InvalidAddress(address) => {
                tonic::Status::invalid_argument(format!("invalid address: {address}"))
            }
            BalanceError::Unavailable(chain) => {
                tonic::Status::unavailable(format!("BLOCKCHAIN UNAVAILABLE: {chain}"))
            }
            BalanceError::Unsupported => {
                tonic::Status::unimplemented("balance tracking is not supported for this asset")
            }
        }
    }
}

/// The chain id named by a request's asset (`asset` or `erc20_asset`), mirroring
/// the legacy `getAnyAssetChain`.
pub fn request_chain_id(request: &BalanceRequest) -> Option<i32> {
    use emerald_api::proto::blockchain::balance_request::BalanceType;
    match request.balance_type.as_ref()? {
        BalanceType::Asset(asset) => Some(asset.chain),
        BalanceType::Erc20Asset(asset) => Some(asset.chain),
    }
}

/// The target blockchain a request addresses, or a [`BalanceError`] when the
/// asset is missing or names an unknown chain.
pub fn request_chain(request: &BalanceRequest) -> Result<TargetBlockchain, BalanceError> {
    let chain_id = request_chain_id(request).ok_or(BalanceError::NoAsset)?;
    TargetBlockchain::try_from(chain_id).map_err(|_| BalanceError::Unavailable(chain_id))
}

/// Resolve an [`AnyAddress`] into the explicit address list to query, or `None`
/// for an xpub (which each asset tracker handles its own way — Bitcoin derives
/// it, others treat it as no match). Addresses are returned in request order;
/// any chain-specific ordering (Bitcoin sorts) is applied by the caller.
pub fn resolve_addresses(
    address: &Option<AnyAddress>,
) -> Result<Option<Vec<String>>, BalanceError> {
    let any = address.as_ref().ok_or(BalanceError::NoAddress)?;
    match any.addr_type.as_ref() {
        Some(AddrType::AddressSingle(single)) => Ok(Some(vec![single.address.clone()])),
        Some(AddrType::AddressMulti(multi)) => {
            Ok(Some(multi.addresses.iter().map(|a| a.address.clone()).collect()))
        }
        Some(AddrType::AddressXpub(_)) => Ok(None),
        _ => Err(BalanceError::NoAddress),
    }
}

/// Validate and canonicalize Ethereum addresses, returning the lowercase
/// `0x`-form the legacy `Address.from(..).toHex()` produced (used for both the
/// upstream calls and the echoed response address). Errors on the first
/// malformed address, matching the legacy throw.
pub fn parse_eth_addresses(addresses: Vec<String>) -> Result<Vec<String>, BalanceError> {
    addresses
        .into_iter()
        .map(|address| {
            address
                .parse::<alloy::primitives::Address>()
                .map(|parsed| format!("{parsed:#x}"))
                .map_err(|_| BalanceError::InvalidAddress(address))
        })
        .collect()
}

/// An empty, successfully-completing balance stream — the legacy response for an
/// address type a tracker simply has nothing to say about (e.g. an xpub on an
/// Ethereum asset).
pub fn empty_stream() -> BalanceStream {
    Box::pin(stream::empty())
}

/// Build an `AddressBalance` for a native asset (the request's `asset` echoed
/// back), with the balance as a base-10 string. `confirmed`/`utxo` stay at their
/// defaults — they only apply to Bitcoin.
pub fn native_balance(asset: Asset, address: String, balance: U256) -> AddressBalance {
    AddressBalance {
        address: Some(SingleAddress { address }),
        balance: balance.to_string(),
        confirmed: false,
        utxo: Vec::new(),
        balance_type: Some(address_balance::BalanceType::Asset(asset)),
    }
}

/// Reads the current [`AddressBalance`] for one address. Each tracker captures
/// its own upstream access, so the shared streaming helpers below don't need to
/// know how a balance is computed. A read failure is surfaced as a
/// `tonic::Status` so the stream fails rather than reporting a fabricated zero
/// balance (legacy lets the reader error propagate).
pub type AddressReader =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<AddressBalance, tonic::Status>> + Send + Sync>;

/// One `AddressBalance` per address with its current value, read once in order.
/// Backs `GetBalance`.
pub fn one_shot(addresses: Vec<String>, reader: AddressReader) -> BalanceStream {
    let stream = stream::iter(addresses).then(move |address| {
        let reader = Arc::clone(&reader);
        async move { reader(address).await }
    });
    Box::pin(stream)
}

/// The current balance per address, then a fresh value whenever it changes on a
/// new head (legacy `distinctUntilChanged { balance }`). Without a head stream
/// this degrades to [`one_shot`]. Backs `SubscribeBalance`.
pub fn subscribe(
    head: Option<Arc<MergedHead>>,
    addresses: Vec<String>,
    reader: AddressReader,
) -> BalanceStream {
    let Some(head) = head else {
        return one_shot(addresses, reader);
    };
    let streams: Vec<BalanceStream> = addresses
        .into_iter()
        .map(|address| address_subscription(head.subscribe(), address, Arc::clone(&reader)))
        .collect();
    Box::pin(stream::select_all(streams))
}

/// Per-address subscription: emit the current balance, then re-read on each new
/// head and emit only when the balance value changed.
fn address_subscription(
    head: broadcast::Receiver<Arc<BlockContainer>>,
    address: String,
    reader: AddressReader,
) -> BalanceStream {
    struct State {
        head: broadcast::Receiver<Arc<BlockContainer>>,
        address: String,
        reader: AddressReader,
        last: Option<String>,
        primed: bool,
        /// Set once a read errors; the next poll ends the stream so the error is
        /// terminal (a balance read failing should fail the subscription).
        done: bool,
    }

    let state = State {
        head,
        address,
        reader,
        last: None,
        primed: false,
        done: false,
    };

    let stream = stream::unfold(state, |mut state| async move {
        loop {
            if state.done {
                return None;
            }
            // The first poll emits the current balance; later ones re-read on a
            // new head and emit only when the balance changed. A read error is
            // emitted once and ends the subscription.
            let trigger = if state.primed {
                state.head.recv().await.map(|_block| ())
            } else {
                state.primed = true;
                Ok(())
            };
            match trigger {
                Ok(()) => match (state.reader)(state.address.clone()).await {
                    Ok(balance) if state.last.as_deref() != Some(balance.balance.as_str()) => {
                        state.last = Some(balance.balance.clone());
                        return Some((Ok(balance), state));
                    }
                    // Unchanged — keep waiting for the next head.
                    Ok(_) => {}
                    Err(status) => {
                        state.done = true;
                        return Some((Err(status), state));
                    }
                },
                // A lagging subscriber skips the gap and re-reads on the next head.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Box::pin(stream)
}

/// Whether `s` is a well-formed 20-byte EVM address (`0x` + 40 hex digits).
/// Used to reject a bogus `erc20_asset.contract_address` up front, mirroring the
/// legacy `Address.isValidAddress` gate in `TrackERC20Address.isSupported`.
pub fn is_valid_eth_address(s: &str) -> bool {
    let digits = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    digits.len() == 40 && digits.chars().all(|c| c.is_ascii_hexdigit())
}

/// Parse a `0x`/`0X`-prefixed hex quantity as a 256-bit integer, defaulting to
/// zero on a missing or malformed value (an upstream returning an error or junk
/// yields a zero balance rather than tearing down the stream).
pub fn parse_hex_u256(hex: &str) -> U256 {
    let digits = hex
        .strip_prefix("0x")
        .or_else(|| hex.strip_prefix("0X"))
        .unwrap_or(hex);
    U256::from_str_radix(digits, 16).unwrap_or(U256::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use emerald_api::proto::common::{Asset, MultiAddress, XpubAddress};

    fn asset_request(chain: i32, code: &str, address: AnyAddress) -> BalanceRequest {
        use emerald_api::proto::blockchain::balance_request::BalanceType;
        BalanceRequest {
            address: Some(address),
            include_utxo: false,
            balance_type: Some(BalanceType::Asset(Asset {
                chain,
                code: code.to_string(),
            })),
        }
    }

    fn single(addr: &str) -> AnyAddress {
        AnyAddress {
            addr_type: Some(AddrType::AddressSingle(SingleAddress {
                address: addr.to_string(),
            })),
        }
    }

    #[test]
    fn chain_comes_from_the_asset() {
        use emerald_api::proto::common::ChainRef;
        let req = asset_request(ChainRef::ChainEthereum as i32, "ether", single("0xabc"));
        assert_eq!(request_chain_id(&req), Some(ChainRef::ChainEthereum as i32));
        assert_eq!(
            request_chain(&req).unwrap(),
            TargetBlockchain::Standard(ChainRef::ChainEthereum)
        );
    }

    #[test]
    fn missing_asset_is_no_asset_error() {
        let req = BalanceRequest {
            address: Some(single("0xabc")),
            include_utxo: false,
            balance_type: None,
        };
        assert_eq!(request_chain(&req).unwrap_err(), BalanceError::NoAsset);
    }

    #[test]
    fn unknown_chain_is_unavailable() {
        let req = asset_request(999_999, "ether", single("0xabc"));
        assert_eq!(
            request_chain(&req).unwrap_err(),
            BalanceError::Unavailable(999_999)
        );
    }

    #[test]
    fn single_address_resolves() {
        let addr = single("0xABC");
        assert_eq!(
            resolve_addresses(&Some(addr)).unwrap(),
            Some(vec!["0xABC".to_string()])
        );
    }

    #[test]
    fn multi_address_keeps_request_order() {
        // Legacy sorts only for Bitcoin; resolution preserves request order.
        let addr = AnyAddress {
            addr_type: Some(AddrType::AddressMulti(MultiAddress {
                addresses: vec![
                    SingleAddress {
                        address: "0xccc".into(),
                    },
                    SingleAddress {
                        address: "0xaaa".into(),
                    },
                ],
            })),
        };
        assert_eq!(
            resolve_addresses(&Some(addr)).unwrap(),
            Some(vec!["0xccc".to_string(), "0xaaa".to_string()])
        );
    }

    #[test]
    fn xpub_resolves_to_none() {
        let addr = AnyAddress {
            addr_type: Some(AddrType::AddressXpub(XpubAddress {
                xpub: "xpub...".into(),
                start: 0,
                limit: 10,
                unused_limit: 20,
            })),
        };
        assert_eq!(resolve_addresses(&Some(addr)).unwrap(), None);
    }

    #[test]
    fn missing_address_is_error() {
        assert_eq!(
            resolve_addresses(&None).unwrap_err(),
            BalanceError::NoAddress
        );
    }

    #[test]
    fn eth_addresses_validated_and_lowercased() {
        // A checksummed address normalizes to lowercase 0x form.
        let parsed =
            parse_eth_addresses(vec!["0x000000000000000000000000000000000000dEaD".into()]).unwrap();
        assert_eq!(parsed, vec!["0x000000000000000000000000000000000000dead"]);
    }

    #[test]
    fn eth_address_invalid_is_rejected() {
        let err = parse_eth_addresses(vec!["0xnothex".into()]).unwrap_err();
        assert_eq!(err, BalanceError::InvalidAddress("0xnothex".to_string()));
    }

    #[test]
    fn hex_parse_handles_prefixes_and_junk() {
        assert_eq!(parse_hex_u256("0x10"), U256::from(16));
        assert_eq!(parse_hex_u256("0X10"), U256::from(16));
        assert_eq!(parse_hex_u256("ff"), U256::from(255));
        assert_eq!(parse_hex_u256("not-hex"), U256::ZERO);
    }
}
