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

//! Ethereum fee estimation.
//!
//! Two response shapes, picked at construction by whether the chain supports
//! EIP-1559 (legacy `EthereumMultistream` choosing `EthereumPriorityFees` vs
//! `EthereumLegacyFees`):
//!
//! - **extended** (EIP-1559): `EthereumExtFees` with `max` / `priority` /
//!   `expect`, derived from each sampled transaction's 1559 fields and the
//!   block's base fee.
//! - **standard** (pre-1559): `EthereumStdFees` carrying the average
//!   `gasPrice`.

use super::{ChainFees, FeeError, FeeMode, block_range};
use crate::jsonrpc::{JsonRpcRequest, RpcMethod};
use crate::upstream::egress::ChainAccess;
use alloy::primitives::U256;
use serde_json::{Value, json};
use std::sync::Arc;

/// Per-transaction fee components, all in Wei. Mirrors the legacy
/// `EthereumFees.EthereumFee` (`max`, `priority`, `paid`, `base`).
#[derive(Clone, Copy)]
struct EthereumFee {
    max: U256,
    priority: U256,
    paid: U256,
    base: U256,
}

/// Ethereum fee estimator. Reads full blocks (with transaction bodies) over the
/// sample window through [`ChainAccess`], extracts one fee per block, and
/// aggregates per the [`FeeMode`].
pub struct EthereumFees {
    access: Arc<dyn ChainAccess>,
    /// Upper bound on how many blocks back a single estimate may sample (legacy
    /// `EthereumMultistream` uses 256).
    height_limit: u32,
    /// Whether to emit EIP-1559 extended fees. Fixed per chain at construction.
    extended: bool,
}

impl EthereumFees {
    /// Build an estimator. `extended` selects the EIP-1559 response shape and
    /// fee math; `height_limit` caps the sample window.
    pub fn new(access: Arc<dyn ChainAccess>, extended: bool, height_limit: u32) -> Self {
        Self {
            access,
            height_limit,
            extended,
        }
    }

    /// Fetch a block with full transaction bodies (`eth_getBlockByNumber`,
    /// `fullTx = true`). Returns `None` on a failed/empty/error response so a
    /// single bad block is skipped rather than failing the whole estimate.
    async fn read_block(&self, height: u64) -> Option<Value> {
        let request = JsonRpcRequest::new(
            0,
            RpcMethod::from("eth_getBlockByNumber"),
            json!([format!("0x{height:x}"), true]),
        );
        let response = self.access.call(&request).await.ok()?;
        if response.error.is_some() {
            return None;
        }
        serde_json::from_str(response.result?.get()).ok()
    }

    /// Read and extract the sampled transaction's fee from the block at `height`.
    async fn fee_at(&self, height: u64, mode: FeeMode) -> Option<EthereumFee> {
        let block = self.read_block(height).await?;
        let txs = block.get("transactions")?.as_array()?;
        let index = mode.tx_index(txs.len())?;
        Some(self.extract_fee(&block, &txs[index]))
    }

    /// Derive the fee components of one transaction. Mirrors
    /// `EthereumPriorityFees.extractFee` (extended) and
    /// `EthereumLegacyFees.extractFee` (standard).
    fn extract_fee(&self, block: &Value, tx: &Value) -> EthereumFee {
        let gas_price = wei(tx.get("gasPrice"));
        if !self.extended {
            // Pre-1559: the only signal is the gas price; the response carries
            // the average of it.
            return EthereumFee {
                max: gas_price,
                priority: gas_price,
                paid: gas_price,
                base: U256::ZERO,
            };
        }

        let base = wei(block.get("baseFeePerGas"));
        if int(tx.get("type")) == 2 {
            // EIP-1559: paid = min(base + priority, max).
            let max = wei(tx.get("maxFeePerGas"));
            let priority = wei(tx.get("maxPriorityFeePerGas"));
            let paid = base.saturating_add(priority).min(max);
            EthereumFee {
                max,
                priority,
                paid,
                base,
            }
        } else {
            // Legacy transaction on a 1559 chain: priority is whatever sits
            // above the base fee.
            EthereumFee {
                max: gas_price,
                priority: gas_price.saturating_sub(base),
                paid: gas_price,
                base,
            }
        }
    }

    /// Render the aggregate as the proto response for this chain's fee shape.
    fn to_response(
        &self,
        fee: EthereumFee,
    ) -> emerald_api::proto::blockchain::EstimateFeeResponse {
        use emerald_api::proto::blockchain::estimate_fee_response::FeeType;
        use emerald_api::proto::blockchain::{
            EstimateFeeResponse, EthereumExtFees, EthereumStdFees,
        };

        let fee_type = if self.extended {
            FeeType::EthereumExtended(EthereumExtFees {
                expect: fee.paid.to_string(),
                priority: fee.priority.to_string(),
                max: fee.max.to_string(),
            })
        } else {
            FeeType::EthereumStd(EthereumStdFees {
                fee: fee.paid.to_string(),
            })
        };
        EstimateFeeResponse {
            fee_type: Some(fee_type),
        }
    }
}

#[async_trait::async_trait]
impl ChainFees for EthereumFees {
    async fn estimate(
        &self,
        mode: FeeMode,
        blocks: u32,
    ) -> Result<emerald_api::proto::blockchain::EstimateFeeResponse, FeeError> {
        let height = self.access.current_height().ok_or(FeeError::NotReady)?;
        let heights = block_range(height, blocks, self.height_limit);
        if heights.is_empty() {
            return Err(FeeError::NoData);
        }

        // Read every block in the window concurrently (legacy `Flux.flatMap`),
        // then collect. One sampled fee per block; blocks with no usable
        // transaction (e.g. an empty block) drop out, matching the legacy
        // per-block `Mono.empty`. Order doesn't matter — both aggregations are
        // order-independent.
        let fees: Vec<EthereumFee> =
            futures::future::join_all(heights.into_iter().map(|height| self.fee_at(height, mode)))
                .await
                .into_iter()
                .flatten()
                .collect();

        let aggregate = aggregate(mode, &fees).ok_or(FeeError::NoData)?;
        Ok(self.to_response(aggregate))
    }
}

/// Combine the per-block fees. `MIN_ALWAYS` takes the per-field maximum (a fee
/// accepted by every block); every other mode averages. Legacy
/// `EthereumFees.feeAggregation`. Returns `None` for an empty set.
fn aggregate(mode: FeeMode, fees: &[EthereumFee]) -> Option<EthereumFee> {
    if fees.is_empty() {
        return None;
    }
    if mode == FeeMode::MinAlways {
        return fees.iter().copied().reduce(|a, b| EthereumFee {
            max: a.max.max(b.max),
            priority: a.priority.max(b.priority),
            paid: a.paid.max(b.paid),
            base: U256::ZERO,
        });
    }

    let count = U256::from(fees.len());
    let mut sum = EthereumFee {
        max: U256::ZERO,
        priority: U256::ZERO,
        paid: U256::ZERO,
        base: U256::ZERO,
    };
    for fee in fees {
        sum.max += fee.max;
        sum.priority += fee.priority;
        sum.paid += fee.paid;
        sum.base += fee.base;
    }
    Some(EthereumFee {
        max: sum.max / count,
        priority: sum.priority / count,
        paid: sum.paid / count,
        base: sum.base / count,
    })
}

/// Parse a `0x`-prefixed hex quantity as Wei, defaulting to zero when missing
/// or malformed (the field genuinely may be absent, e.g. `baseFeePerGas` on a
/// pre-1559 block).
fn wei(value: Option<&Value>) -> U256 {
    value
        .and_then(Value::as_str)
        .and_then(|s| U256::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(U256::ZERO)
}

/// Parse a `0x`-prefixed hex quantity as a small integer (transaction `type`).
fn int(value: Option<&Value>) -> u64 {
    value
        .and_then(Value::as_str)
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::traits::UpstreamError;
    use emerald_api::proto::blockchain::estimate_fee_response::FeeType;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A `ChainAccess` that serves canned `eth_getBlockByNumber` results keyed
    /// by height, plus a fixed current height.
    struct FakeChain {
        height: Option<u64>,
        blocks: Mutex<HashMap<u64, Value>>,
    }

    impl FakeChain {
        fn new(height: Option<u64>) -> Self {
            Self {
                height,
                blocks: Mutex::new(HashMap::new()),
            }
        }

        fn with_block(self, height: u64, block: Value) -> Self {
            self.blocks.lock().unwrap().insert(height, block);
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
        async fn call(
            &self,
            request: &JsonRpcRequest,
        ) -> Result<JsonRpcResponse, UpstreamError> {
            // params: ["0x<height>", true]
            let height_hex = request.params[0].as_str().unwrap().trim_start_matches("0x");
            let height = u64::from_str_radix(height_hex, 16).unwrap();
            let result = self
                .blocks
                .lock()
                .unwrap()
                .get(&height)
                .cloned()
                .unwrap_or(Value::Null);
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                serde_json::to_string(&result).unwrap()
            );
            Ok(serde_json::from_str(&body).unwrap())
        }
    }

    /// Block with the given base fee and transactions (each a JSON object).
    fn block(base_fee: &str, txs: Vec<Value>) -> Value {
        json!({ "baseFeePerGas": base_fee, "transactions": txs })
    }

    fn legacy_tx(gas_price: &str) -> Value {
        json!({ "type": "0x0", "gasPrice": gas_price })
    }

    fn tx_1559(max: &str, priority: &str) -> Value {
        json!({ "type": "0x2", "maxFeePerGas": max, "maxPriorityFeePerGas": priority })
    }

    async fn estimate(
        chain: FakeChain,
        extended: bool,
        mode: FeeMode,
        blocks: u32,
    ) -> Result<emerald_api::proto::blockchain::EstimateFeeResponse, FeeError> {
        let fees = EthereumFees::new(Arc::new(chain), extended, 256);
        fees.estimate(mode, blocks).await
    }

    #[tokio::test]
    async fn standard_fee_averages_gas_price() {
        // Two blocks, last tx gas prices 0x10 (16) and 0x20 (32) → avg 24.
        let chain = FakeChain::new(Some(11))
            .with_block(10, block("0x0", vec![legacy_tx("0x10")]))
            .with_block(11, block("0x0", vec![legacy_tx("0x20")]));
        let resp = estimate(chain, false, FeeMode::AvgLast, 2).await.unwrap();
        match resp.fee_type.unwrap() {
            FeeType::EthereumStd(std) => assert_eq!(std.fee, "24"),
            other => panic!("expected std fee, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn extended_fee_computes_eip1559_paid() {
        // base 0x5 (5), maxPriority 0x3 (3), max 0x100 (256):
        // paid = min(5 + 3, 256) = 8.
        let chain =
            FakeChain::new(Some(10)).with_block(10, block("0x5", vec![tx_1559("0x100", "0x3")]));
        let resp = estimate(chain, true, FeeMode::AvgLast, 1).await.unwrap();
        match resp.fee_type.unwrap() {
            FeeType::EthereumExtended(ext) => {
                assert_eq!(ext.expect, "8");
                assert_eq!(ext.priority, "3");
                assert_eq!(ext.max, "256");
            }
            other => panic!("expected extended fee, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn extended_fee_paid_is_capped_at_max() {
        // base 0x100 (256) + priority 0x50 (80) = 336, but max is 0x120 (288).
        let chain =
            FakeChain::new(Some(10)).with_block(10, block("0x100", vec![tx_1559("0x120", "0x50")]));
        let resp = estimate(chain, true, FeeMode::AvgLast, 1).await.unwrap();
        match resp.fee_type.unwrap() {
            FeeType::EthereumExtended(ext) => assert_eq!(ext.expect, "288"),
            other => panic!("expected extended fee, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn min_always_takes_the_highest() {
        // gas prices 10 and 50 → MIN_ALWAYS reports the max, 50.
        let chain = FakeChain::new(Some(11))
            .with_block(10, block("0x0", vec![legacy_tx("0xa")]))
            .with_block(11, block("0x0", vec![legacy_tx("0x32")]));
        let resp = estimate(chain, false, FeeMode::MinAlways, 2).await.unwrap();
        match resp.fee_type.unwrap() {
            FeeType::EthereumStd(std) => assert_eq!(std.fee, "50"),
            other => panic!("expected std fee, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_blocks_are_skipped() {
        // Only one of the two blocks has a transaction.
        let chain = FakeChain::new(Some(11))
            .with_block(10, block("0x0", vec![]))
            .with_block(11, block("0x0", vec![legacy_tx("0x14")]));
        let resp = estimate(chain, false, FeeMode::AvgLast, 2).await.unwrap();
        match resp.fee_type.unwrap() {
            FeeType::EthereumStd(std) => assert_eq!(std.fee, "20"),
            other => panic!("expected std fee, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_height_is_not_ready() {
        let chain = FakeChain::new(None);
        assert_eq!(
            estimate(chain, false, FeeMode::AvgLast, 2).await.unwrap_err(),
            FeeError::NotReady
        );
    }

    #[tokio::test]
    async fn no_transactions_anywhere_is_no_data() {
        let chain = FakeChain::new(Some(10)).with_block(10, block("0x0", vec![]));
        assert_eq!(
            estimate(chain, false, FeeMode::AvgLast, 1).await.unwrap_err(),
            FeeError::NoData
        );
    }
}
