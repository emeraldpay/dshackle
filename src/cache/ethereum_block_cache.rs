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

//! Ethereum-specific [`BlockCacheCodec`] implementation.
//!
//! Handles `eth_getBlockByHash` — caches blocks requested with
//! `full_transactions=false` (tx hashes only, not full tx objects).

use crate::cache::caching_upstream::BlockCacheCodec;
use crate::data::{BlockContainer, BlockId};
use crate::upstream::ethereum::head::parse_eth_block;

/// Ethereum block cache codec.
///
/// Cacheable: `eth_getBlockByHash(hash, false)`
/// Not cacheable: full-transaction requests (`true`) or any other method.
pub struct EthereumBlockCache;

impl BlockCacheCodec for EthereumBlockCache {
    fn cacheable_block_hash(&self, method: &str, params: &serde_json::Value) -> Option<BlockId> {
        if method != "eth_getBlockByHash" {
            return None;
        }
        let arr = params.as_array()?;
        if requests_full_transactions(arr.get(1)) {
            return None;
        }
        arr.first()?.as_str()?.parse().ok()
    }

    fn parse_block_response(&self, method: &str, raw_json: &str) -> Option<BlockContainer> {
        if method != "eth_getBlockByHash" {
            return None;
        }
        parse_eth_block(raw_json)
    }
}

/// Check whether the second parameter requests full transaction objects.
/// We only cache blocks with tx hashes, not full tx bodies.
fn requests_full_transactions(param: Option<&serde_json::Value>) -> bool {
    param.and_then(|v| v.as_bool()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_block_by_hash_cacheable() {
        let codec = EthereumBlockCache;
        let params = serde_json::json!([
            "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75",
            false
        ]);
        assert!(codec
            .cacheable_block_hash("eth_getBlockByHash", &params)
            .is_some());
    }

    #[test]
    fn get_block_by_hash_without_flag_cacheable() {
        let codec = EthereumBlockCache;
        let params = serde_json::json!([
            "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75"
        ]);
        assert!(codec
            .cacheable_block_hash("eth_getBlockByHash", &params)
            .is_some());
    }

    #[test]
    fn get_block_by_hash_full_tx_not_cacheable() {
        let codec = EthereumBlockCache;
        let params = serde_json::json!([
            "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75",
            true
        ]);
        assert!(codec
            .cacheable_block_hash("eth_getBlockByHash", &params)
            .is_none());
    }

    #[test]
    fn get_block_by_number_not_handled() {
        let codec = EthereumBlockCache;
        let params = serde_json::json!(["0xc5043f", false]);
        assert!(codec
            .cacheable_block_hash("eth_getBlockByNumber", &params)
            .is_none());
    }

    #[test]
    fn unknown_method_not_cacheable() {
        let codec = EthereumBlockCache;
        let params = serde_json::json!(["0x1"]);
        assert!(codec
            .cacheable_block_hash("eth_getBalance", &params)
            .is_none());
    }

    #[test]
    fn full_tx_true() {
        assert!(requests_full_transactions(Some(&serde_json::json!(true))));
    }

    #[test]
    fn full_tx_false() {
        assert!(!requests_full_transactions(Some(&serde_json::json!(false))));
    }

    #[test]
    fn full_tx_absent_defaults_to_false() {
        assert!(!requests_full_transactions(None));
    }
}
