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

//! Unspent outputs read from an Esplora address index, porting the legacy
//! `EsploraUnspentReader` and `AddressActiveCheck`.
//!
//! Esplora indexes every address, so unlike the node's `listunspent` it
//! answers for addresses nobody imported anywhere, and it knows an address's
//! history: an xpub scan can tell a used-then-emptied address from a
//! never-used one, and reports the former with a zero balance.

use super::unspent::{Unspent, UnspentSource};
use crate::upstream::bitcoin::esplora::{EsploraClient, EsploraError};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;

/// Requests in flight per read. Esplora is asked address by address; this
/// keeps a wide read fast without flooding a single index with one client's
/// request.
const CONCURRENCY: usize = 8;

/// Addresses one xpub scan may examine, the legacy cap. Each of them is a
/// request to the index, two when it is in use, so a wider window is paged by
/// the client instead of being walked inside a single call.
const SCAN_LIMIT: u32 = 100;

/// An Esplora instance as a source of unspent outputs.
pub struct EsploraUnspentSource {
    client: Arc<EsploraClient>,
}

impl EsploraUnspentSource {
    /// A source reading from the index behind `client`. The client is shared:
    /// one is built per chain at startup and reused by every request.
    pub fn new(client: Arc<EsploraClient>) -> Self {
        Self { client }
    }

    /// Confirmed unspent outputs of one address. Mempool outputs are left out
    /// so both sources agree on what a balance is: `listunspent` is asked for
    /// at least one confirmation.
    async fn address_unspent(&self, address: &str) -> Result<Vec<Unspent>, tonic::Status> {
        let outputs = self.client.utxo(address).await.map_err(unavailable)?;
        Ok(outputs
            .into_iter()
            .filter(|output| output.status.confirmed)
            .map(|output| Unspent {
                address: address.to_string(),
                tx_id: output.txid,
                index: output.vout,
                value: output.value,
            })
            .collect())
    }

    /// The unspent outputs of `address` when it has any history, `None` for
    /// an address that never appeared on chain.
    async fn used_address(&self, address: &str) -> Result<Option<Vec<Unspent>>, tonic::Status> {
        let active = self
            .client
            .has_transactions(address)
            .await
            .map_err(unavailable)?;
        if !active {
            return Ok(None);
        }
        self.address_unspent(address).await.map(Some)
    }
}

#[async_trait::async_trait]
impl UnspentSource for EsploraUnspentSource {
    async fn unspent(&self, addresses: &[String]) -> Result<Vec<Unspent>, tonic::Status> {
        let reads: Vec<_> = addresses
            .iter()
            .map(|address| self.address_unspent(address))
            .collect();
        let per_address: Vec<Vec<Unspent>> = stream::iter(reads)
            .buffered(CONCURRENCY)
            .try_collect()
            .await?;
        Ok(per_address.into_iter().flatten().collect())
    }

    /// In use means the address has transaction history, whether or not
    /// anything is still unspent on it.
    async fn used(
        &self,
        addresses: &[String],
    ) -> Result<HashMap<String, Vec<Unspent>>, tonic::Status> {
        let checks: Vec<_> = addresses
            .iter()
            .map(|address| self.used_address(address))
            .collect();
        // `buffered` keeps the order, so the answers line up with `addresses`.
        let answers: Vec<Option<Vec<Unspent>>> = stream::iter(checks)
            .buffered(CONCURRENCY)
            .try_collect()
            .await?;
        Ok(addresses
            .iter()
            .zip(answers)
            .filter_map(|(address, outputs)| Some((address.clone(), outputs?)))
            .collect())
    }

    /// Every examined address is a request of its own, so a round never goes
    /// past the point where the gap rule could already have stopped the scan.
    fn scan_round(&self, until_gap: u32) -> u32 {
        until_gap
    }

    fn scan_limit(&self) -> Option<u32> {
        Some(SCAN_LIMIT)
    }
}

fn unavailable(error: EsploraError) -> tonic::Status {
    tonic::Status::unavailable(format!("balance read failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockchain::TargetBlockchain;
    use crate::upstream::balance::BalanceStream;
    use crate::upstream::balance::BitcoinBalance;
    use crate::upstream::balance::xpub_scan::{ScanRequest, scan};
    use crate::upstream::bitcoin::esplora_stub::StubEsplora;
    use crate::upstream::bitcoin::xpub::XpubAddresses;
    use emerald_api::proto::blockchain::AddressBalance;
    use emerald_api::proto::common::{ChainRef, XpubAddress};
    use std::time::Duration;
    use tokio_stream::StreamExt;

    // The same key the derivation tests use (legacy `XpubAddressesSpec`).
    const MAINNET_ZPUB: &str = "zpub6tsHyQGj1UmDcw9UavB31HSChH9rqMi8Qje5KPbnczzhFSZykmm3afivpVytxWubaabDih6AbGGhioiTCg5PssYXuVGiZ1vTVaMYvvQvvmz";

    const MAINNET: TargetBlockchain = TargetBlockchain::Standard(ChainRef::ChainBitcoin);

    const CONFIRMED: &str = r#"{"confirmed":true,"block_height":647721,"block_time":1599808442}"#;

    fn source(esplora: &StubEsplora) -> Arc<EsploraUnspentSource> {
        let http = crate::tls::reqwest_client(None, Duration::from_secs(5), false).unwrap();
        let client = EsploraClient::new(esplora.url(), None, http).unwrap();
        Arc::new(EsploraUnspentSource::new(Arc::new(client)))
    }

    /// Derived address at `index`.
    fn derived(index: u32) -> String {
        let key: XpubAddresses = MAINNET_ZPUB.parse().unwrap();
        key.addresses(index, 1).unwrap()[0].to_string()
    }

    /// Give `address` a transaction history.
    fn mark_used(esplora: &StubEsplora, address: &str) {
        esplora.respond(&format!("/address/{address}/txs"), r#"[{"txid":"aa"}]"#);
    }

    /// Give `address` a history and one confirmed output of `value` satoshis.
    fn fund(esplora: &StubEsplora, address: &str, value: u64) {
        mark_used(esplora, address);
        esplora.respond(
            &format!("/address/{address}/utxo"),
            &format!(r#"[{{"txid":"aa","vout":0,"status":{CONFIRMED},"value":{value}}}]"#),
        );
    }

    fn scan_request(start: u64, limit: u64) -> ScanRequest {
        let request = XpubAddress {
            xpub: MAINNET_ZPUB.to_string(),
            start,
            limit,
            unused_limit: 0,
        };
        ScanRequest::parse(&request, MAINNET).unwrap()
    }

    /// How many addresses had their history checked.
    fn history_checks(esplora: &StubEsplora) -> usize {
        esplora
            .requests()
            .iter()
            .filter(|r| r.path.ends_with("/txs"))
            .count()
    }

    async fn drain(mut stream: BalanceStream) -> Vec<AddressBalance> {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn sums_outputs_of_an_address() {
        let esplora = StubEsplora::start().await;
        esplora.respond(
            "/address/addr/utxo",
            &format!(
                r#"[{{"txid":"aa","vout":0,"status":{CONFIRMED},"value":500}},
                    {{"txid":"bb","vout":3,"status":{CONFIRMED},"value":250}}]"#
            ),
        );

        let tracker =
            BitcoinBalance::new(source(&esplora), None, ChainRef::ChainBitcoin as i32, true);
        let out = drain(tracker.get_balance(vec!["addr".into()])).await;

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].balance, "750");
        assert_eq!(out[0].utxo.len(), 2);
        assert_eq!(out[0].utxo[1].tx_id, "bb");
        assert_eq!(out[0].utxo[1].index, 3);
        assert_eq!(out[0].utxo[1].balance, "250");
    }

    #[tokio::test]
    async fn address_unknown_to_esplora_has_zero_balance() {
        let esplora = StubEsplora::start().await;

        let tracker =
            BitcoinBalance::new(source(&esplora), None, ChainRef::ChainBitcoin as i32, true);
        let out = drain(tracker.get_balance(vec!["addr".into()])).await;

        assert_eq!(out[0].balance, "0");
        assert!(out[0].utxo.is_empty());
    }

    #[tokio::test]
    async fn mempool_outputs_are_not_counted() {
        let esplora = StubEsplora::start().await;
        esplora.respond(
            "/address/addr/utxo",
            &format!(
                r#"[{{"txid":"aa","vout":0,"status":{CONFIRMED},"value":500}},
                    {{"txid":"bb","vout":0,"status":{{"confirmed":false}},"value":250}}]"#
            ),
        );

        let outputs = source(&esplora).unspent(&["addr".into()]).await.unwrap();

        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].tx_id, "aa");
    }

    #[tokio::test]
    async fn failed_read_is_an_error_not_a_zero_balance() {
        let esplora = StubEsplora::start().await;
        esplora.fail("/address/addr/utxo", 500);

        let status = source(&esplora)
            .unspent(&["addr".into()])
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn scan_reports_used_addresses_including_emptied_ones() {
        let esplora = StubEsplora::start().await;
        fund(&esplora, &derived(0), 1_000);
        // Received and fully spent: history, nothing unspent.
        mark_used(&esplora, &derived(3));

        let source = source(&esplora);
        let found = scan(source.as_ref(), &scan_request(0, 100)).await.unwrap();
        let tracker = BitcoinBalance::new(source, None, ChainRef::ChainBitcoin as i32, false);
        let out = drain(tracker.get_balance_scanned(found)).await;

        let balances: Vec<(String, &str)> = out
            .iter()
            .map(|b| (b.address.clone().unwrap().address, b.balance.as_str()))
            .collect();
        assert_eq!(balances, [(derived(0), "1000"), (derived(3), "0")]);
    }

    #[tokio::test]
    async fn scan_of_an_unused_key_stops_at_the_gap() {
        let esplora = StubEsplora::start().await;

        let found = scan(source(&esplora).as_ref(), &scan_request(0, 100))
            .await
            .unwrap();

        assert!(found.is_empty());
        // Offsets 0..=20, as the legacy `activeAddresses` examined.
        assert_eq!(history_checks(&esplora), 21);
    }

    #[tokio::test]
    async fn scan_continues_while_addresses_are_in_use() {
        let esplora = StubEsplora::start().await;
        // 40 is past the first round and found only because 25 keeps the scan
        // going; 70 lies more than a gap after the last used address.
        for index in [5, 25, 40, 70] {
            mark_used(&esplora, &derived(index));
        }

        let found = scan(source(&esplora).as_ref(), &scan_request(0, 1000))
            .await
            .unwrap();

        let addresses: Vec<String> = found.into_iter().map(|a| a.address).collect();
        assert_eq!(addresses, [derived(5), derived(25), derived(40)]);
        // Offsets 0..=60: exactly 20 unused ones after the last used.
        assert_eq!(history_checks(&esplora), 61);
    }

    #[tokio::test]
    async fn scan_is_cut_to_the_legacy_window() {
        let esplora = StubEsplora::start().await;
        for index in 0..120 {
            mark_used(&esplora, &derived(index));
        }

        let found = scan(source(&esplora).as_ref(), &scan_request(0, 1000))
            .await
            .unwrap();

        assert_eq!(found.len(), 100);
        assert_eq!(found[99].address, derived(99));
        assert_eq!(history_checks(&esplora), 100);
    }

    #[tokio::test]
    async fn scan_fails_when_esplora_does() {
        let esplora = StubEsplora::start().await;
        esplora.fail(&format!("/address/{}/txs", derived(2)), 502);

        let status = scan(source(&esplora).as_ref(), &scan_request(0, 100))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::Unavailable);
    }
}
