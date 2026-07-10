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

//! Bitcoin fee estimation.
//!
//! Bitcoin has no per-transaction fee field — a transaction's fee is the
//! difference between what its inputs spend and what its outputs pay. For each
//! sampled block we pick one transaction (per the [`FeeMode`], skipping the
//! coinbase), resolve every input's value by reading the prior transaction it
//! spends, and normalize the resulting fee to satoshi-per-kilo-vbyte. The
//! per-block rates are then averaged (or maxed for `MIN_ALWAYS`). Ports the
//! legacy `BitcoinFees`.

use super::{ChainFees, FeeError, FeeMode, block_range};
use crate::data::TxId;
use crate::upstream::bitcoin::btc_to_satoshis;
use crate::upstream::bitcoin::reader::BitcoinReader;
use emerald_api::proto::blockchain::EstimateFeeResponse;
use futures::future::join_all;
use serde_json::Value;

/// A sampled fee rate (satoshi per kilo-vbyte) and how many samples it
/// represents, so several blocks' rates can be summed and averaged. Mirrors the
/// legacy `BitcoinFees.TxFee`.
#[derive(Clone, Copy)]
struct TxFee {
    count: u64,
    fee: u64,
}

/// Bitcoin fee estimator. Reads blocks and transactions through a
/// [`BitcoinReader`] and derives fees from input/output amount differences.
pub struct BitcoinFees {
    reader: BitcoinReader,
    /// Upper bound on how many blocks back a single estimate may sample (legacy
    /// `BitcoinMultistream` uses 6).
    height_limit: u32,
}

impl BitcoinFees {
    /// Build an estimator reading through `reader`, sampling at most
    /// `height_limit` blocks back.
    pub fn new(reader: BitcoinReader, height_limit: u32) -> Self {
        Self {
            reader,
            height_limit,
        }
    }

    /// Sample one transaction's fee rate from the block at `height`, or `None`
    /// when the block has no eligible transaction or the fee can't be resolved.
    async fn fee_at(&self, height: u64, mode: FeeMode) -> Option<TxFee> {
        let block = self.reader.block_by_height(height).await?;
        let txids = selectable_txids(&block);
        let index = mode.tx_index(txids.len())?;
        let tx = self.reader.tx(&txids[index]).await?;

        let size = tx_size(&tx)?;
        if size == 0 {
            return None;
        }
        let fee = self.transaction_fee(&tx).await?;
        // Normalize to satoshi-per-kilo-vbyte (legacy `fee * 1024 / vsize`).
        Some(TxFee {
            count: 1,
            fee: fee.saturating_mul(1024) / size,
        })
    }

    /// Total fee of a transaction in satoshis: the sum of its input amounts
    /// (each resolved by reading the prior transaction it spends) minus the sum
    /// of its output amounts. `None` when the transaction has no inputs or none
    /// can be resolved. Legacy `BitcoinFees.calculateFee`.
    async fn transaction_fee(&self, tx: &Value) -> Option<u64> {
        let out_amount: u64 = vout_amounts(tx).into_iter().flatten().sum();

        let vins = tx.get("vin")?.as_array()?;
        let inputs = join_all(vins.iter().map(|vin| self.input_amount(vin))).await;

        // The fee is undefined unless at least one input resolves — matching the
        // legacy reduce over a flux that yields nothing when no prior output is
        // readable (a coinbase or fully-unreadable transaction is skipped).
        let mut in_amount = 0u64;
        let mut resolved = false;
        for amount in inputs.into_iter().flatten() {
            in_amount = in_amount.saturating_add(amount);
            resolved = true;
        }
        if !resolved {
            return None;
        }
        Some(in_amount.saturating_sub(out_amount))
    }

    /// The amount (satoshis) of the prior output a transaction input spends, by
    /// reading that prior transaction. `None` when the input has no outpoint
    /// (e.g. coinbase) or the prior output can't be read.
    async fn input_amount(&self, vin: &Value) -> Option<u64> {
        let txid: TxId = vin.get("txid")?.as_str()?.parse().ok()?;
        let vout = vin.get("vout")?.as_u64()? as usize;
        let prev = self.reader.tx(&txid).await?;
        vout_amounts(&prev).get(vout).copied().flatten()
    }
}

#[async_trait::async_trait]
impl ChainFees for BitcoinFees {
    async fn estimate(&self, mode: FeeMode, blocks: u32) -> Result<EstimateFeeResponse, FeeError> {
        let height = self.reader.current_height().ok_or(FeeError::NotReady)?;
        let heights = block_range(height, blocks, self.height_limit);
        if heights.is_empty() {
            return Err(FeeError::NoData);
        }

        // Sample every block in the window concurrently; blocks with no usable
        // transaction drop out (legacy per-block `Mono.empty`).
        let fees: Vec<TxFee> =
            join_all(heights.into_iter().map(|height| self.fee_at(height, mode)))
                .await
                .into_iter()
                .flatten()
                .collect();

        let aggregate = aggregate(mode, &fees).ok_or(FeeError::NoData)?;
        Ok(to_response(aggregate))
    }
}

/// Transaction ids eligible for fee sampling: the block's `tx` list with the
/// coinbase (the first entry) dropped, since it pays no fee. Legacy
/// `BitcoinFees.extractTx`. The coinbase is dropped by position before parsing,
/// so an unparsable entry can't shift the offset.
fn selectable_txids(block: &Value) -> Vec<TxId> {
    let Some(txs) = block.get("tx").and_then(Value::as_array) else {
        return Vec::new();
    };
    let hashes: Vec<&str> = txs.iter().filter_map(Value::as_str).collect();
    let without_coinbase = if hashes.is_empty() {
        &hashes[..]
    } else {
        &hashes[1..]
    };
    without_coinbase
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect()
}

/// Output amounts of a transaction in satoshis, by output index; `None` for an
/// output without a numeric `value`. Legacy `BitcoinFees.extractVOuts`.
fn vout_amounts(tx: &Value) -> Vec<Option<u64>> {
    let Some(vouts) = tx.get("vout").and_then(Value::as_array) else {
        return Vec::new();
    };
    vouts
        .iter()
        .map(|v| v.get("value").and_then(btc_to_satoshis))
        .collect()
}

/// Virtual size of a transaction in vbytes, preferring `vsize` then falling back
/// to `size`. Legacy `BitcoinFees.extractSize`.
fn tx_size(tx: &Value) -> Option<u64> {
    tx.get("vsize")
        .or_else(|| tx.get("size"))
        .and_then(Value::as_u64)
}

/// Combine the per-block fee rates. `MIN_ALWAYS` keeps the highest rate (a fee
/// every sampled block accepted); every other mode sums for an average. Legacy
/// `BitcoinFees.feeAggregation`. `None` for an empty sample set.
fn aggregate(mode: FeeMode, fees: &[TxFee]) -> Option<TxFee> {
    if fees.is_empty() {
        return None;
    }
    if mode == FeeMode::MinAlways {
        return fees
            .iter()
            .copied()
            .reduce(|a, b| if a.fee > b.fee { a } else { b });
    }
    Some(TxFee {
        count: fees.iter().map(|f| f.count).sum(),
        fee: fees.iter().map(|f| f.fee).sum(),
    })
}

/// Render the aggregate as the Bitcoin fee response: the average rate, at least
/// 1 sat/KvB. Legacy `BitcoinFees.getResponseBuilder`.
fn to_response(fee: TxFee) -> EstimateFeeResponse {
    use emerald_api::proto::blockchain::BitcoinStdFees;
    use emerald_api::proto::blockchain::estimate_fee_response::FeeType;

    let sat_per_kb = (fee.fee / fee.count).max(1);
    EstimateFeeResponse {
        fee_type: Some(FeeType::BitcoinStd(BitcoinStdFees { sat_per_kb })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::egress::ChainAccess;
    use crate::upstream::traits::UpstreamError;
    use emerald_api::proto::blockchain::estimate_fee_response::FeeType;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    // 32-byte hex ids (the reader parses `tx` entries and `vin.txid` as `TxId`).
    const COINBASE: &str = "00000000000000000000000000000000000000000000000000000000000000aa";
    const TXID_A: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const TXID_PREV: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    const BLOCK_HASH: &str = "3333333333333333333333333333333333333333333333333333333333333333";

    /// A `ChainAccess` that answers by `(method, first-param)`, so the input
    /// resolution (which reads several transactions by id) can be exercised.
    struct FakeChain {
        height: Option<u64>,
        responses: Mutex<HashMap<String, Value>>,
    }

    impl FakeChain {
        fn new(height: Option<u64>) -> Self {
            Self {
                height,
                responses: Mutex::new(HashMap::new()),
            }
        }

        fn with(self, method: &str, param: &str, result: Value) -> Self {
            self.responses
                .lock()
                .unwrap()
                .insert(format!("{method}:{param}"), result);
            self
        }
    }

    #[async_trait::async_trait]
    impl ChainAccess for FakeChain {
        fn is_syncing(&self) -> bool {
            false
        }
        fn current_height(&self) -> Option<u64> {
            self.height
        }
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let param = match &request.params[0] {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let key = format!("{}:{}", request.method.as_str(), param);
            let result = self
                .responses
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or(Value::Null);
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                serde_json::to_string(&result).unwrap()
            );
            Ok(serde_json::from_str(&body).unwrap())
        }
    }

    fn fees_with(chain: FakeChain) -> BitcoinFees {
        let reader = BitcoinReader::new(Arc::new(chain), None);
        BitcoinFees::new(reader, 6)
    }

    fn sat_per_kb(resp: EstimateFeeResponse) -> u64 {
        match resp.fee_type.unwrap() {
            FeeType::BitcoinStd(f) => f.sat_per_kb,
            other => panic!("expected bitcoin fee, got {other:?}"),
        }
    }

    /// One block, one fee-paying tx spending 1.0 BTC and paying out 0.9 BTC, so
    /// the fee is 0.1 BTC = 10_000_000 sat over a 200-vbyte tx →
    /// 10_000_000 * 1024 / 200 = 51_200_000 sat/KvB.
    #[tokio::test]
    async fn estimates_fee_from_input_minus_output() {
        let chain = FakeChain::new(Some(100))
            .with("getblockhash", "100", json!(BLOCK_HASH))
            .with(
                "getblock",
                BLOCK_HASH,
                json!({ "hash": BLOCK_HASH, "height": 100, "time": 1, "tx": [COINBASE, TXID_A] }),
            )
            .with(
                "getrawtransaction",
                TXID_A,
                json!({
                    "vsize": 200,
                    "vin": [{ "txid": TXID_PREV, "vout": 0 }],
                    "vout": [{ "value": 0.9 }]
                }),
            )
            .with(
                "getrawtransaction",
                TXID_PREV,
                json!({ "vout": [{ "value": 1.0 }] }),
            );

        let resp = fees_with(chain)
            .estimate(FeeMode::AvgLast, 1)
            .await
            .expect("estimate");
        assert_eq!(sat_per_kb(resp), 51_200_000);
    }

    /// The coinbase (first tx) is never sampled — AVG_TOP picks the first
    /// *non-coinbase* tx (legacy "doesn't count COINBASE as top tx").
    #[tokio::test]
    async fn coinbase_is_skipped_for_top() {
        let chain = FakeChain::new(Some(100))
            .with("getblockhash", "100", json!(BLOCK_HASH))
            .with(
                "getblock",
                BLOCK_HASH,
                json!({ "hash": BLOCK_HASH, "height": 100, "time": 1, "tx": [COINBASE, TXID_A] }),
            )
            .with(
                "getrawtransaction",
                TXID_A,
                json!({
                    "vsize": 1024,
                    "vin": [{ "txid": TXID_PREV, "vout": 0 }],
                    "vout": [{ "value": 0.0 }]
                }),
            )
            .with(
                "getrawtransaction",
                TXID_PREV,
                json!({ "vout": [{ "value": 0.000_05 }] }),
            );

        // 5000 sat fee over 1024 vbytes → 5000 * 1024 / 1024 = 5000 sat/KvB.
        let resp = fees_with(chain)
            .estimate(FeeMode::AvgTop, 1)
            .await
            .expect("estimate");
        assert_eq!(sat_per_kb(resp), 5000);
    }

    #[tokio::test]
    async fn block_with_only_coinbase_yields_no_data() {
        let chain = FakeChain::new(Some(100))
            .with("getblockhash", "100", json!(BLOCK_HASH))
            .with(
                "getblock",
                BLOCK_HASH,
                json!({ "hash": BLOCK_HASH, "height": 100, "time": 1, "tx": [COINBASE] }),
            );
        assert_eq!(
            fees_with(chain)
                .estimate(FeeMode::AvgLast, 1)
                .await
                .unwrap_err(),
            FeeError::NoData
        );
    }

    #[tokio::test]
    async fn no_height_is_not_ready() {
        let chain = FakeChain::new(None);
        assert_eq!(
            fees_with(chain)
                .estimate(FeeMode::AvgLast, 1)
                .await
                .unwrap_err(),
            FeeError::NotReady
        );
    }

    #[test]
    fn btc_to_satoshis_is_exact() {
        assert_eq!(btc_to_satoshis(&json!(1.0)), Some(100_000_000));
        assert_eq!(btc_to_satoshis(&json!(0.1)), Some(10_000_000));
        assert_eq!(btc_to_satoshis(&json!(0.000_000_01)), Some(1));
        assert_eq!(
            btc_to_satoshis(&json!(21_000_000.0)),
            Some(2_100_000_000_000_000)
        );
        assert_eq!(btc_to_satoshis(&Value::Null), None);
    }

    #[test]
    fn tx_size_prefers_vsize() {
        assert_eq!(tx_size(&json!({ "vsize": 200, "size": 800 })), Some(200));
        assert_eq!(tx_size(&json!({ "size": 800 })), Some(800));
        assert_eq!(tx_size(&json!({})), None);
    }

    #[test]
    fn min_always_keeps_highest_rate() {
        let fees = [
            TxFee { count: 1, fee: 10 },
            TxFee { count: 1, fee: 50 },
            TxFee { count: 1, fee: 30 },
        ];
        let agg = aggregate(FeeMode::MinAlways, &fees).unwrap();
        assert_eq!(sat_per_kb(to_response(agg)), 50);
    }

    #[test]
    fn average_divides_total_by_count() {
        let fees = [TxFee { count: 1, fee: 10 }, TxFee { count: 1, fee: 20 }];
        let agg = aggregate(FeeMode::AvgLast, &fees).unwrap();
        // (10 + 20) / 2 = 15.
        assert_eq!(sat_per_kb(to_response(agg)), 15);
    }
}
