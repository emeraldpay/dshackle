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

//! Bitcoin-specific [`BlockCacheCodec`] implementation.
//!
//! Handles `getblock` — caches blocks requested with verbosity=1
//! (JSON with tx hashes, not raw hex or full tx objects).

use crate::cache::caching_upstream::BlockCacheCodec;
use crate::data::{BlockContainer, BlockId};
use crate::upstream::bitcoin::head::parse_btc_block;

/// Bitcoin block cache codec.
///
/// Cacheable: `getblock(hash, 1)` (verbosity=1 is the default).
/// Not cacheable: verbosity=0 (raw hex) or verbosity=2 (full tx objects).
pub struct BitcoinBlockCache;

impl BlockCacheCodec for BitcoinBlockCache {
    fn cacheable_block_hash(&self, method: &str, params: &serde_json::Value) -> Option<BlockId> {
        if method != "getblock" {
            return None;
        }
        let arr = params.as_array()?;
        // verbosity=1 returns JSON with tx hashes (the format we cache)
        let verbosity = arr.get(1).and_then(|v| v.as_u64()).unwrap_or(1);
        if verbosity != 1 {
            return None;
        }
        arr.first()?.as_str()?.parse().ok()
    }

    fn parse_block_response(&self, method: &str, raw_json: &str) -> Option<BlockContainer> {
        if method != "getblock" {
            return None;
        }
        parse_btc_block(raw_json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getblock_verbosity_1_cacheable() {
        let codec = BitcoinBlockCache;
        let params = serde_json::json!([
            "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6",
            1
        ]);
        assert!(codec.cacheable_block_hash("getblock", &params).is_some());
    }

    #[test]
    fn getblock_default_verbosity_cacheable() {
        // When verbosity is omitted, bitcoind defaults to 1
        let codec = BitcoinBlockCache;
        let params = serde_json::json!([
            "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6"
        ]);
        assert!(codec.cacheable_block_hash("getblock", &params).is_some());
    }

    #[test]
    fn getblock_verbosity_0_not_cacheable() {
        let codec = BitcoinBlockCache;
        let params = serde_json::json!([
            "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6",
            0
        ]);
        assert!(codec.cacheable_block_hash("getblock", &params).is_none());
    }

    #[test]
    fn getblock_verbosity_2_not_cacheable() {
        let codec = BitcoinBlockCache;
        let params = serde_json::json!([
            "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6",
            2
        ]);
        assert!(codec.cacheable_block_hash("getblock", &params).is_none());
    }

    #[test]
    fn unknown_method_not_cacheable() {
        let codec = BitcoinBlockCache;
        let params = serde_json::json!(["abc"]);
        assert!(codec
            .cacheable_block_hash("getblockhash", &params)
            .is_none());
    }
}
