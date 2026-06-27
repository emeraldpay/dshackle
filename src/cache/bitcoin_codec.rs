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

//! Bitcoin-specific [`CacheCodec`] implementation.
//!
//! Handles `getblock` — caches blocks requested with verbosity=1
//! (JSON with tx hashes, not raw hex or full tx objects). Transactions and
//! full blocks (verbosity=2) are not cached yet.

use crate::cache::caching_upstream::{CacheCodec, CacheUpdate, CacheableCall};
use crate::data::{BlockContainer, TxContainer};
use crate::upstream::bitcoin::head::parse_btc_block;

/// Bitcoin cache codec.
pub struct BitcoinCacheCodec;

impl CacheCodec for BitcoinCacheCodec {
    fn classify(&self, method: &str, params: &serde_json::Value) -> Option<CacheableCall> {
        if method != "getblock" {
            return None;
        }
        let arr = params.as_array()?;
        // verbosity=1 returns JSON with tx hashes (the format we cache)
        let verbosity = arr.get(1).and_then(|v| v.as_u64()).unwrap_or(1);
        if verbosity != 1 {
            return None;
        }
        Some(CacheableCall::Block(arr.first()?.as_str()?.parse().ok()?))
    }

    fn parse_response(&self, call: &CacheableCall, raw_json: &str) -> Option<CacheUpdate> {
        match call {
            CacheableCall::Block(_) => parse_btc_block(raw_json).map(CacheUpdate::Block),
            // classify never produces these for Bitcoin
            CacheableCall::FullBlock(_) | CacheableCall::Tx(_) | CacheableCall::Receipt(_) => None,
        }
    }

    fn rebuild_full_block(&self, _block: &BlockContainer, _txs: &[TxContainer]) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getblock_verbosity_1_cacheable() {
        let codec = BitcoinCacheCodec;
        let params = serde_json::json!([
            "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6",
            1
        ]);
        assert!(matches!(
            codec.classify("getblock", &params),
            Some(CacheableCall::Block(_))
        ));
    }

    #[test]
    fn getblock_default_verbosity_cacheable() {
        // When verbosity is omitted, bitcoind defaults to 1
        let codec = BitcoinCacheCodec;
        let params =
            serde_json::json!(["00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6"]);
        assert!(codec.classify("getblock", &params).is_some());
    }

    #[test]
    fn getblock_verbosity_0_not_cacheable() {
        let codec = BitcoinCacheCodec;
        let params = serde_json::json!([
            "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6",
            0
        ]);
        assert!(codec.classify("getblock", &params).is_none());
    }

    #[test]
    fn getblock_verbosity_2_not_cacheable() {
        let codec = BitcoinCacheCodec;
        let params = serde_json::json!([
            "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6",
            2
        ]);
        assert!(codec.classify("getblock", &params).is_none());
    }

    #[test]
    fn unknown_method_not_cacheable() {
        let codec = BitcoinCacheCodec;
        let params = serde_json::json!(["abc"]);
        assert!(codec.classify("getblockhash", &params).is_none());
    }
}
