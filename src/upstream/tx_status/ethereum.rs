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

//! Ethereum transaction status reader. Ports the read side of the legacy
//! `TrackEthereumTx`: `eth_getTransactionByHash` tells whether the transaction
//! is seen and (via `blockNumber`/`blockHash`) mined; the block's timestamp
//! comes from a follow-up `eth_getBlockByNumber`.

use super::{Ttl, TxReader, TxState, confirmations};
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::egress::ChainAccess;
use crate::upstream::ethereum::parse_hex_quantity;
use alloy::primitives::{B256, TxHash, U64};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;

/// TTL cutoffs for Ethereum tracking (legacy `TrackEthereumTx`): give up a
/// minute after a never-seen transaction, two minutes after a seen-but-unmined
/// one, and cap the whole subscription at an hour.
pub fn ttl() -> Ttl {
    Ttl {
        overall: Some(Duration::from_secs(60 * 60)),
        not_found: Some(Duration::from_secs(60)),
        not_mined: Some(Duration::from_secs(2 * 60)),
    }
}

/// Reads an Ethereum transaction's status via JSON-RPC.
pub struct EthereumTxReader {
    access: Arc<dyn ChainAccess>,
    /// The requested hash, parsed once. `None` when the id was malformed; like
    /// Bitcoin, that is reported as not-found rather than erroring.
    hash: Option<TxHash>,
    /// Echoed id: lowercase hex without the `0x` prefix (legacy `TxId.toHex`).
    tx_id: String,
}

impl EthereumTxReader {
    pub fn new(access: Arc<dyn ChainAccess>, tx_id: String) -> Self {
        let hash = tx_id.parse::<TxHash>().ok();
        Self {
            access,
            hash,
            // A valid hash echoes in canonical form; a malformed one falls back
            // to the best-effort normalized input so the response still names it:
            // drop any `0x` prefix and lowercase.
            tx_id: hash.map_or_else(
                || {
                    tx_id
                        .strip_prefix("0x")
                        .or_else(|| tx_id.strip_prefix("0X"))
                        .unwrap_or(&tx_id)
                        .to_lowercase()
                },
                |h| format!("{h:x}"),
            ),
        }
    }

    /// The including block's timestamp in epoch milliseconds, read separately
    /// since the transaction object carries no timestamp. A failure leaves it
    /// unset rather than failing the whole read.
    async fn block_time_millis(&self, height: u64) -> Option<u64> {
        let request = JsonRpcRequest::new(
            0,
            "eth_getBlockByNumber".into(),
            json!([format!("0x{height:x}"), false]),
        );
        let response = self.access.call(&request).await.ok()?;
        let block: Value = serde_json::from_str(response.result?.get()).ok()?;
        let seconds = parse_hex_quantity(block.get("timestamp")?.as_str()?)?;
        Some(seconds * 1000)
    }
}

#[async_trait::async_trait]
impl TxReader for EthereumTxReader {
    fn tx_id(&self) -> &str {
        &self.tx_id
    }

    async fn read(&self) -> Result<TxState, tonic::Status> {
        let head_height = self.access.current_height();
        let Some(hash) = self.hash else {
            // A malformed id can never be found — report it as such rather than
            // erroring, matching the Bitcoin reader.
            return Ok(TxState::not_found());
        };
        let request = JsonRpcRequest::new(
            0,
            "eth_getTransactionByHash".into(),
            json!([format!("{hash:#x}")]),
        );
        let response = self
            .access
            .call(&request)
            .await
            .map_err(|e| tonic::Status::unavailable(format!("tx read failed: {e}")))?;
        if let Some(error) = &response.error {
            return Err(tonic::Status::unavailable(format!(
                "upstream error reading transaction: {}",
                error.message
            )));
        }
        let Some(result) = response.result else {
            return Ok(TxState::not_found());
        };
        let tx: Value = serde_json::from_str(result.get())
            .map_err(|e| tonic::Status::internal(format!("invalid transaction result: {e}")))?;
        if tx.is_null() {
            return Ok(TxState::not_found());
        }

        // A non-null transaction is broadcast. It's mined once it carries a block
        // number and a non-zero block hash.
        let block_hash = tx
            .get("blockHash")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<B256>().ok());
        let height = tx
            .get("blockNumber")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<U64>().ok())
            .map(|n| n.to::<u64>());
        let (Some(block_hash), Some(height)) = (block_hash, height) else {
            return Ok(TxState::pending());
        };
        if block_hash.is_zero() {
            return Ok(TxState::pending());
        }

        Ok(TxState {
            found: true,
            mined: true,
            height: Some(height),
            // Legacy renders the block id as lowercase hex without the `0x`.
            block_id: Some(format!("{block_hash:x}")),
            block_time: self.block_time_millis(height).await,
            confirmations: confirmations(head_height, height),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::traits::UpstreamError;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A `ChainAccess` serving canned results by method, with a fixed head.
    struct FakeChain {
        head: Option<u64>,
        results: Mutex<HashMap<String, Value>>,
    }

    impl FakeChain {
        fn new(head: Option<u64>) -> Arc<Self> {
            Arc::new(Self {
                head,
                results: Mutex::new(HashMap::new()),
            })
        }
        fn with(self: &Arc<Self>, method: &str, result: Value) {
            self.results
                .lock()
                .unwrap()
                .insert(method.to_string(), result);
        }
    }

    #[async_trait::async_trait]
    impl ChainAccess for FakeChain {
        fn is_syncing(&self) -> bool {
            false
        }
        fn current_height(&self) -> Option<u64> {
            self.head
        }
        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let result = self
                .results
                .lock()
                .unwrap()
                .get(request.method.as_str())
                .cloned()
                .unwrap_or(Value::Null);
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#,
                serde_json::to_string(&result).unwrap()
            );
            Ok(serde_json::from_str(&body).unwrap())
        }
    }

    // A full 32-byte tx hash in mixed case, to exercise canonicalization.
    const TX: &str = "0xaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaBaB";
    const TX_CANON: &str = "abababababababababababababababababababababababababababababababab";

    fn reader(chain: Arc<FakeChain>) -> EthereumTxReader {
        EthereumTxReader::new(chain, TX.to_string())
    }

    #[tokio::test]
    async fn unknown_tx_is_not_found() {
        let chain = FakeChain::new(Some(100));
        // eth_getTransactionByHash returns null.
        let state = reader(chain).read().await.unwrap();
        assert!(!state.found);
        assert!(!state.mined);
    }

    #[tokio::test]
    async fn seen_but_unmined_is_pending() {
        let chain = FakeChain::new(Some(100));
        chain.with(
            "eth_getTransactionByHash",
            json!({ "blockHash": Value::Null, "blockNumber": Value::Null }),
        );
        let state = reader(chain).read().await.unwrap();
        assert!(state.found);
        assert!(!state.mined);
        assert_eq!(state.confirmations, 0);
    }

    // A full 32-byte block hash and its `block_id` rendering (lowercase, no `0x`).
    const BLOCK_HASH: &str = "0x00000000000000000000000000000000000000000000000000000000DeadBeef";
    const ZERO_BLOCK_HASH: &str =
        "0x0000000000000000000000000000000000000000000000000000000000000000";

    #[tokio::test]
    async fn mined_tx_reports_block_and_confirmations() {
        let chain = FakeChain::new(Some(105));
        chain.with(
            "eth_getTransactionByHash",
            json!({ "blockHash": BLOCK_HASH, "blockNumber": "0x64" }), // height 100
        );
        chain.with("eth_getBlockByNumber", json!({ "timestamp": "0x6553f100" }));
        let state = reader(chain).read().await.unwrap();
        assert!(state.mined);
        assert_eq!(state.height, Some(100));
        // The `0x` is dropped and the mixed-case input is canonicalized to lowercase.
        assert_eq!(
            state.block_id.as_deref(),
            Some("00000000000000000000000000000000000000000000000000000000deadbeef")
        );
        // 105 - 100 + 1 = 6 confirmations.
        assert_eq!(state.confirmations, 6);
        // 0x6553f100 seconds → millis.
        assert_eq!(state.block_time, Some(0x6553_f100 * 1000));
    }

    #[tokio::test]
    async fn zero_block_hash_is_not_mined() {
        let chain = FakeChain::new(Some(100));
        chain.with(
            "eth_getTransactionByHash",
            json!({ "blockHash": ZERO_BLOCK_HASH, "blockNumber": "0x64" }),
        );
        let state = reader(chain).read().await.unwrap();
        assert!(state.found);
        assert!(!state.mined);
    }

    #[tokio::test]
    async fn read_failure_errors() {
        struct ErrorChain;
        #[async_trait::async_trait]
        impl ChainAccess for ErrorChain {
            fn is_syncing(&self) -> bool {
                false
            }
            async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
                Err(UpstreamError::Transport("down".into()))
            }
        }
        let reader = EthereumTxReader::new(Arc::new(ErrorChain), TX.to_string());
        assert!(reader.read().await.is_err());
    }

    #[test]
    fn tx_id_is_canonicalized() {
        let chain = FakeChain::new(None);
        // Parsed as a TxHash, then rendered 0x-stripped and lowercased.
        assert_eq!(reader(chain).tx_id(), TX_CANON);
    }

    #[tokio::test]
    async fn malformed_tx_id_is_not_found() {
        // A non-32-byte id can't be a real hash: reported as not-found (as for
        // Bitcoin), and echoed back best-effort rather than erroring.
        let chain = FakeChain::new(Some(100));
        let reader = EthereumTxReader::new(chain, "0xABBA".to_string());
        assert_eq!(reader.tx_id(), "abba");
        let state = reader.read().await.unwrap();
        assert!(!state.found);
    }
}
