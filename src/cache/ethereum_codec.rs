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

//! Ethereum-specific [`CacheCodec`] implementation.
//!
//! Cacheable calls:
//! - `eth_getBlockByHash(hash, false)` — block with tx hashes;
//! - `eth_getBlockByHash(hash, true)` — full block, cached as block + txs and
//!   re-assembled from those parts on a hit;
//! - `eth_getTransactionByHash(hash)` — single transaction.

use crate::cache::caching_upstream::{CacheCodec, CacheUpdate, CacheableCall};
use crate::cache::ethereum_full_block;
use crate::data::{BlockContainer, TxContainer};
use crate::upstream::ethereum::head::parse_eth_block;

/// Ethereum cache codec.
pub struct EthereumCacheCodec;

impl CacheCodec for EthereumCacheCodec {
    fn classify(&self, method: &str, params: &serde_json::Value) -> Option<CacheableCall> {
        match method {
            "eth_getBlockByHash" => {
                let arr = params.as_array()?;
                let hash = arr.first()?.as_str()?.parse().ok()?;
                if requests_full_transactions(arr.get(1)) {
                    Some(CacheableCall::FullBlock(hash))
                } else {
                    Some(CacheableCall::Block(hash))
                }
            }
            "eth_getTransactionByHash" => {
                let arr = params.as_array()?;
                Some(CacheableCall::Tx(arr.first()?.as_str()?.parse().ok()?))
            }
            _ => None,
        }
    }

    fn parse_response(&self, call: &CacheableCall, raw_json: &str) -> Option<CacheUpdate> {
        match call {
            CacheableCall::Block(_) => parse_eth_block(raw_json).map(CacheUpdate::Block),
            CacheableCall::FullBlock(_) => ethereum_full_block::decompose(raw_json)
                .map(|(block, txs)| CacheUpdate::FullBlock { block, txs }),
            CacheableCall::Tx(_) => ethereum_full_block::parse_eth_tx(raw_json).map(CacheUpdate::Tx),
        }
    }

    fn rebuild_full_block(&self, block: &BlockContainer, txs: &[TxContainer]) -> Option<String> {
        let block_json = std::str::from_utf8(block.json.as_deref()?).ok()?;
        ethereum_full_block::reconstruct(block_json, txs)
    }
}

/// Check whether the second parameter requests full transaction objects.
fn requests_full_transactions(param: Option<&serde_json::Value>) -> bool {
    param.and_then(|v| v.as_bool()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK_HASH: &str = "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";
    const TX_HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn block_by_hash_classified_as_block() {
        let codec = EthereumCacheCodec;
        let params = serde_json::json!([BLOCK_HASH, false]);
        assert!(matches!(
            codec.classify("eth_getBlockByHash", &params),
            Some(CacheableCall::Block(_))
        ));
    }

    #[test]
    fn block_by_hash_without_flag_classified_as_block() {
        let codec = EthereumCacheCodec;
        let params = serde_json::json!([BLOCK_HASH]);
        assert!(matches!(
            codec.classify("eth_getBlockByHash", &params),
            Some(CacheableCall::Block(_))
        ));
    }

    #[test]
    fn block_by_hash_full_classified_as_full_block() {
        let codec = EthereumCacheCodec;
        let params = serde_json::json!([BLOCK_HASH, true]);
        assert!(matches!(
            codec.classify("eth_getBlockByHash", &params),
            Some(CacheableCall::FullBlock(_))
        ));
    }

    #[test]
    fn tx_by_hash_classified_as_tx() {
        let codec = EthereumCacheCodec;
        let params = serde_json::json!([TX_HASH]);
        assert!(matches!(
            codec.classify("eth_getTransactionByHash", &params),
            Some(CacheableCall::Tx(_))
        ));
    }

    #[test]
    fn block_by_number_not_classified() {
        let codec = EthereumCacheCodec;
        let params = serde_json::json!(["0xc5043f", false]);
        assert!(codec.classify("eth_getBlockByNumber", &params).is_none());
    }

    #[test]
    fn unknown_method_not_classified() {
        let codec = EthereumCacheCodec;
        let params = serde_json::json!(["0x1"]);
        assert!(codec.classify("eth_getBalance", &params).is_none());
    }

    #[test]
    fn invalid_hash_not_classified() {
        let codec = EthereumCacheCodec;
        let params = serde_json::json!(["latest", false]);
        assert!(codec.classify("eth_getBlockByHash", &params).is_none());
    }

    #[test]
    fn parses_tx_response() {
        let codec = EthereumCacheCodec;
        let call = codec
            .classify("eth_getTransactionByHash", &serde_json::json!([TX_HASH]))
            .unwrap();
        let raw = format!(
            r#"{{"hash":"{TX_HASH}","blockHash":"{BLOCK_HASH}","blockNumber":"0x64"}}"#
        );

        let update = codec.parse_response(&call, &raw).unwrap();

        match update {
            CacheUpdate::Tx(tx) => {
                assert_eq!(tx.hash.to_hex_prefixed(), TX_HASH);
                assert_eq!(tx.height, Some(0x64));
            }
            _ => panic!("expected a Tx update"),
        }
    }

    #[test]
    fn parses_full_block_response_into_parts() {
        let codec = EthereumCacheCodec;
        let call = codec
            .classify(
                "eth_getBlockByHash",
                &serde_json::json!([BLOCK_HASH, true]),
            )
            .unwrap();
        let raw = format!(
            r#"{{"hash":"{BLOCK_HASH}","number":"0x64","timestamp":"0x65a8b44c","transactions":[{{"hash":"{TX_HASH}","blockHash":"{BLOCK_HASH}","blockNumber":"0x64"}}]}}"#
        );

        let update = codec.parse_response(&call, &raw).unwrap();

        match update {
            CacheUpdate::FullBlock { block, txs } => {
                assert_eq!(block.height, 0x64);
                assert_eq!(txs.len(), 1);
            }
            _ => panic!("expected a FullBlock update"),
        }
    }
}
