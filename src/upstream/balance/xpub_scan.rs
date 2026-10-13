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

//! Xpub balance scan: find the addresses of a key that are in use.
//!
//! Derives the xpub's addresses round by round and asks the chain's
//! [`UnspentSource`] which of them are in use, following the wallet gap rule:
//! the scan continues while one of the last `unused_limit` (default 20)
//! derived addresses is in use. Ports the legacy `XpubAddresses.activeAddresses`.
//!
//! What "in use" means, and how many addresses a round takes, depends on the
//! source. Esplora knows an address's history, so the result includes
//! used-then-emptied addresses (with a zero balance), as legacy reported them.
//! The node's `listunspent` only knows what is currently unspent, so there the
//! result holds funded addresses only, and a run of emptied addresses longer
//! than the gap ends the scan early.
//!
//! `limit` caps the total addresses examined. All request parameters are
//! validated into a [`ScanRequest`] before any upstream is contacted, with
//! hard ceilings on `limit` and `unused_limit` so a single request cannot
//! drive an unbounded number of upstream calls. A source that pays per address
//! narrows the window further (Esplora keeps the legacy cap of 100), while the
//! node's `listunspent` takes a hundred addresses per call and gets the whole
//! validated window.

use super::BalanceError;
use super::bitcoin::network_for;
use super::unspent::{Unspent, UnspentSource};
use crate::blockchain::TargetBlockchain;
use crate::upstream::bitcoin::xpub::XpubAddresses;
use emerald_api::proto::common::XpubAddress;

/// Gap limit when the request doesn't set `unused_limit` (the legacy
/// `INACTIVE_LIMIT`).
const DEFAULT_UNUSED_LIMIT: u64 = 20;

/// Ceiling for a client-supplied `unused_limit`. The BIP-44 gap standard is
/// 20; five times that is already a generous tolerance, and an unbounded value
/// would let a single used address keep the scan running forever.
const MAX_UNUSED_LIMIT: u64 = 100;

/// Ceiling for `limit`: enough for any real wallet window, small enough that
/// one request stays bounded.
const MAX_LIMIT: u64 = 100_000;

/// One past the last derivable child index (non-hardened space is `0..2^31`).
const DERIVABLE_END: u64 = 1 << 31;

/// One in-use address found by the scan, with the outputs backing its balance
/// (none for an address that was used and emptied).
#[derive(Debug)]
pub struct UsedAddress {
    pub(super) address: String,
    pub(super) unspent: Vec<Unspent>,
}

/// A validated scan: the parsed key plus the request parameters checked into
/// their acceptable ranges, so the scan itself can no longer fail on client
/// input — any error past this point is an upstream one.
#[derive(Debug)]
pub struct ScanRequest {
    key: XpubAddresses,
    start: u32,
    /// Total addresses to examine, already clamped to the derivable space.
    cap: u32,
    gap: u64,
}

impl ScanRequest {
    /// Validate the request for the queried chain. A key whose prefix belongs
    /// to the other network is rejected up front — deriving it would only
    /// produce addresses the node has never seen, i.e. confident empty results
    /// (legacy derived them anyway; failing loudly is the safer behavior).
    pub fn parse(request: &XpubAddress, chain: TargetBlockchain) -> Result<Self, BalanceError> {
        let key: XpubAddresses = request
            .xpub
            .parse()
            .map_err(|_| BalanceError::InvalidAddress(request.xpub.clone()))?;
        if key.network() != network_for(chain) {
            return Err(BalanceError::InvalidAddress(request.xpub.clone()));
        }
        if request.start >= DERIVABLE_END {
            return Err(BalanceError::InvalidRequest(format!(
                "xpub start {} is out of the derivable range",
                request.start
            )));
        }
        if request.limit > MAX_LIMIT {
            return Err(BalanceError::InvalidRequest(format!(
                "xpub limit {} exceeds the maximum of {MAX_LIMIT}",
                request.limit
            )));
        }
        if request.unused_limit > MAX_UNUSED_LIMIT {
            return Err(BalanceError::InvalidRequest(format!(
                "xpub unused_limit {} exceeds the maximum of {MAX_UNUSED_LIMIT}",
                request.unused_limit
            )));
        }
        // Legacy floors the limit to 1; the cap also cannot reach past the
        // derivable space, so the scan never has to handle overflow.
        let cap = request.limit.max(1).min(DERIVABLE_END - request.start) as u32;
        let gap = if request.unused_limit == 0 {
            DEFAULT_UNUSED_LIMIT
        } else {
            request.unused_limit
        };
        Ok(Self {
            key,
            start: request.start as u32,
            cap,
            gap,
        })
    }
}

/// Scan the key's address sequence and return the in-use addresses within the
/// validated window, in derivation order.
pub async fn scan(
    source: &dyn UnspentSource,
    request: &ScanRequest,
) -> Result<Vec<UsedAddress>, tonic::Status> {
    let mut found = Vec::new();
    let mut scanned: u32 = 0;
    // Offset (from `start`) of the last in-use address seen, driving the gap
    // rule across round boundaries.
    let mut last_used: Option<u32> = None;
    let cap = source
        .scan_limit()
        .map_or(request.cap, |limit| request.cap.min(limit));

    while scanned < cap {
        // How far the next address lies from the last in-use one. Before
        // anything is found the gap is counted from the window start (legacy
        // seeded `lastActive = 0`).
        let idle = u64::from(scanned - last_used.unwrap_or(0));
        if idle > request.gap {
            break;
        }
        // `gap` is bounded by `MAX_UNUSED_LIMIT`, so this fits `u32`.
        let until_gap = (request.gap + 1 - idle) as u32;
        let round = source.scan_round(until_gap).clamp(1, cap - scanned);
        let addresses: Vec<String> = request
            .key
            .addresses(request.start + scanned, round)
            // Unreachable after `ScanRequest::parse` bounded the window; a
            // failure here is a bug, not a client mistake.
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .iter()
            .map(|a| a.to_string())
            .collect();
        let mut used = source.used(&addresses).await?;
        for (offset, address) in addresses.into_iter().enumerate() {
            if let Some(unspent) = used.remove(&address) {
                last_used = Some(scanned + offset as u32);
                found.push(UsedAddress { address, unspent });
            }
        }
        scanned += round;
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::balance::rpc_unspent::RpcUnspentSource;
    use crate::upstream::egress::ChainAccess;
    use crate::upstream::traits::UpstreamError;
    use emerald_api::proto::common::ChainRef;
    use serde_json::{Value, json};
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    // The same key the derivation tests use (legacy `XpubAddressesSpec`).
    const MAINNET_ZPUB: &str = "zpub6tsHyQGj1UmDcw9UavB31HSChH9rqMi8Qje5KPbnczzhFSZykmm3afivpVytxWubaabDih6AbGGhioiTCg5PssYXuVGiZ1vTVaMYvvQvvmz";
    const TESTNET_VPUB: &str = "vpub5aEsY5bNGvjkHPjdMiz8FbdoiT81kmF3JwuJ1LRdoFb7DxNh1qAHQCmMeDnr6RG2EcCwzGohem3oESxZGa6YWLPW79ryCyMrdYj54uUzNNq";

    const MAINNET: TargetBlockchain = TargetBlockchain::Standard(ChainRef::ChainBitcoin);

    /// A `ChainAccess` that answers `listunspent` with one 1-BTC output for
    /// every requested address in its funded set, and records the size of each
    /// batch it served.
    struct FakeChain {
        funded: HashSet<String>,
        batches: Mutex<Vec<usize>>,
    }

    impl FakeChain {
        fn new(funded: impl IntoIterator<Item = String>) -> Arc<Self> {
            Arc::new(Self {
                funded: funded.into_iter().collect(),
                batches: Mutex::new(Vec::new()),
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
            let params: Vec<Value> = serde_json::from_value(request.params.clone()).unwrap();
            let addresses: Vec<String> = serde_json::from_value(params[2].clone()).unwrap();
            self.batches.lock().unwrap().push(addresses.len());
            let outputs: Vec<Value> = addresses
                .iter()
                .filter(|a| self.funded.contains(*a))
                .map(|a| json!({ "txid": "aa", "vout": 0, "address": a, "amount": 1.0 }))
                .collect();
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                serde_json::to_string(&outputs).unwrap()
            );
            Ok(serde_json::from_str(&body).unwrap())
        }
    }

    fn key() -> XpubAddresses {
        MAINNET_ZPUB.parse().unwrap()
    }

    /// Derived address at `index`, to seed the fake node's funded set.
    fn derived(index: u32) -> String {
        key().addresses(index, 1).unwrap()[0].to_string()
    }

    fn request(start: u64, limit: u64, unused_limit: u64) -> XpubAddress {
        XpubAddress {
            xpub: MAINNET_ZPUB.to_string(),
            start,
            limit,
            unused_limit,
        }
    }

    fn source(chain: &Arc<FakeChain>) -> RpcUnspentSource {
        RpcUnspentSource::new(Arc::clone(chain) as Arc<dyn ChainAccess>)
    }

    async fn run(chain: &Arc<FakeChain>, req: &XpubAddress) -> Vec<String> {
        let scan_request = ScanRequest::parse(req, MAINNET).unwrap();
        scan(&source(chain), &scan_request)
            .await
            .unwrap()
            .into_iter()
            .map(|f| f.address)
            .collect()
    }

    #[tokio::test]
    async fn finds_funded_addresses_in_first_batch() {
        let chain = FakeChain::new([derived(0), derived(5)]);
        let found = run(&chain, &request(0, 100, 0)).await;
        assert_eq!(found, [derived(0), derived(5)]);
        // Everything funded sits far before the trailing gap — one batch only.
        assert_eq!(*chain.batches.lock().unwrap(), [100]);
    }

    #[tokio::test]
    async fn continues_past_batch_while_tail_is_funded() {
        // 95 is within the last 20 of the first batch, so the scan continues
        // and finds 130 in the second.
        let chain = FakeChain::new([derived(95), derived(130)]);
        let found = run(&chain, &request(0, 300, 0)).await;
        assert_eq!(found, [derived(95), derived(130)]);
        // 130 is not within the last 20 of the second batch — stop at 200.
        assert_eq!(*chain.batches.lock().unwrap(), [100, 100]);
    }

    #[tokio::test]
    async fn stops_when_nothing_is_funded() {
        let chain = FakeChain::new([]);
        let found = run(&chain, &request(0, 1000, 0)).await;
        assert!(found.is_empty());
        assert_eq!(*chain.batches.lock().unwrap(), [100]);
    }

    #[tokio::test]
    async fn limit_caps_the_scan() {
        // 130 is funded but past the cap — never derived, never seen.
        let chain = FakeChain::new([derived(95), derived(130)]);
        let found = run(&chain, &request(0, 120, 0)).await;
        assert_eq!(found, [derived(95)]);
        assert_eq!(*chain.batches.lock().unwrap(), [100, 20]);
    }

    #[tokio::test]
    async fn zero_limit_scans_one_address() {
        // Legacy `max(1, limit)`.
        let chain = FakeChain::new([derived(0)]);
        let found = run(&chain, &request(0, 0, 0)).await;
        assert_eq!(found, [derived(0)]);
        assert_eq!(*chain.batches.lock().unwrap(), [1]);
    }

    #[tokio::test]
    async fn start_offsets_the_window() {
        let chain = FakeChain::new([derived(0), derived(12)]);
        let found = run(&chain, &request(10, 100, 0)).await;
        // Address 0 is outside the window even though it's funded.
        assert_eq!(found, [derived(12)]);
    }

    #[tokio::test]
    async fn custom_unused_limit_extends_the_gap() {
        // 70 is not within the last 20 of the first batch, but within a
        // 40-address gap window.
        let chain = FakeChain::new([derived(70), derived(105)]);
        let default_gap = FakeChain::new([derived(70), derived(105)]);
        assert_eq!(
            run(&chain, &request(0, 300, 40)).await,
            [derived(70), derived(105)]
        );
        assert_eq!(run(&default_gap, &request(0, 300, 0)).await, [derived(70)]);
    }

    #[tokio::test]
    async fn gap_reaches_past_an_empty_first_batch() {
        // Nothing funded in the first batch, but the 100-address gap window
        // still covers the start of the second — the scan must not give up
        // before it (legacy counts the gap from the window start).
        let chain = FakeChain::new([derived(120)]);
        let found = run(&chain, &request(0, 300, 100)).await;
        assert_eq!(found, [derived(120)]);
        assert_eq!(*chain.batches.lock().unwrap(), [100, 100, 100]);
    }

    #[tokio::test]
    async fn cap_is_clamped_to_the_derivable_space() {
        // The window touches the 2^31 boundary: scan the remainder, no error.
        let chain = FakeChain::new([]);
        let start = (1u64 << 31) - 5;
        let scan_request = ScanRequest::parse(&request(start, 100_000, 0), MAINNET).unwrap();
        assert!(
            scan(&source(&chain), &scan_request)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(*chain.batches.lock().unwrap(), [5]);
    }

    #[test]
    fn start_past_derivable_range_is_rejected() {
        for start in [1u64 << 31, u64::from(u32::MAX), u64::MAX] {
            let err = ScanRequest::parse(&request(start, 10, 0), MAINNET).unwrap_err();
            assert!(matches!(err, BalanceError::InvalidRequest(_)), "{start}");
        }
    }

    #[test]
    fn excessive_limits_are_rejected() {
        assert!(matches!(
            ScanRequest::parse(&request(0, MAX_LIMIT + 1, 0), MAINNET).unwrap_err(),
            BalanceError::InvalidRequest(_)
        ));
        assert!(matches!(
            ScanRequest::parse(&request(0, 100, MAX_UNUSED_LIMIT + 1), MAINNET).unwrap_err(),
            BalanceError::InvalidRequest(_)
        ));
        // The maxima themselves are accepted.
        assert!(ScanRequest::parse(&request(0, MAX_LIMIT, MAX_UNUSED_LIMIT), MAINNET).is_ok());
    }

    #[test]
    fn rejects_key_of_the_wrong_network() {
        let err = ScanRequest::parse(
            &XpubAddress {
                xpub: TESTNET_VPUB.to_string(),
                ..Default::default()
            },
            MAINNET,
        )
        .unwrap_err();
        assert!(matches!(err, BalanceError::InvalidAddress(k) if k == TESTNET_VPUB));
    }

    #[test]
    fn rejects_malformed_key() {
        let err = ScanRequest::parse(
            &XpubAddress {
                xpub: "not-an-xpub".to_string(),
                ..Default::default()
            },
            MAINNET,
        )
        .unwrap_err();
        assert!(matches!(err, BalanceError::InvalidAddress(_)));
    }
}
