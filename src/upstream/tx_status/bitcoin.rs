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

//! Bitcoin transaction status reader. Ports the read side of the legacy
//! `TrackBitcoinTx`: `getrawtransaction` tells whether the transaction is seen
//! (mempool or block) and, via its `blockhash`, mined; the including block is
//! read for its height and time.

use super::{Ttl, TxReader, TxState, confirmations};
use crate::data::{BlockId, TxId};
use crate::upstream::bitcoin::reader::BitcoinReader;
use serde_json::Value;
use std::time::Duration;

/// TTL cutoffs for Bitcoin tracking (legacy `TrackBitcoinTx`): wait up to ten
/// minutes for a never-seen transaction to appear. There's no not-mined or
/// overall cutoff — a seen transaction is tracked until it reaches the
/// confirmation limit (Bitcoin blocks are ~10 minutes apart).
pub fn ttl() -> Ttl {
    Ttl {
        overall: None,
        not_found: Some(Duration::from_secs(10 * 60)),
        not_mined: None,
    }
}

/// Reads a Bitcoin transaction's status through a [`BitcoinReader`].
pub struct BitcoinTxReader {
    reader: BitcoinReader,
    /// Echoed transaction id (lowercase hex, no prefix — Bitcoin's native form).
    tx_id: String,
}

impl BitcoinTxReader {
    pub fn new(reader: BitcoinReader, tx_id: String) -> Self {
        Self {
            tx_id: tx_id.to_lowercase(),
            reader,
        }
    }
}

#[async_trait::async_trait]
impl TxReader for BitcoinTxReader {
    fn tx_id(&self) -> &str {
        &self.tx_id
    }

    async fn read(&self) -> Result<TxState, tonic::Status> {
        let head_height = self.reader.current_height();
        let Ok(txid) = self.tx_id.parse::<TxId>() else {
            // A malformed id can never be found — report it as such rather than
            // erroring, matching the legacy poll-until-found flow.
            return Ok(TxState::not_found());
        };

        // The node returns nothing for a transaction it hasn't seen.
        let Some(tx) = self.reader.tx(&txid).await else {
            return Ok(TxState::not_found());
        };

        // A `blockhash` means it's mined; without one it's still in the mempool.
        let Some(block_hash) = tx.get("blockhash").and_then(Value::as_str) else {
            return Ok(TxState::pending());
        };

        // Read the including block for its height and time.
        let block = match block_hash.parse::<BlockId>() {
            Ok(hash) => self.reader.block_by_hash(&hash).await,
            Err(_) => None,
        };
        let height = block
            .as_ref()
            .and_then(|b| b.get("height"))
            .and_then(Value::as_u64);
        let block_time = block
            .as_ref()
            .and_then(|b| b.get("time"))
            .and_then(Value::as_u64)
            .map(|seconds| seconds * 1000);

        Ok(TxState {
            found: true,
            mined: true,
            height,
            block_id: Some(block_hash.to_string()),
            block_time,
            confirmations: height.map_or(1, |h| confirmations(head_height, h)),
        })
    }
}
