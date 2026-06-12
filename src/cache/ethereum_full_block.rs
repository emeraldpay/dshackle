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

//! Splitting and re-assembling Ethereum blocks with full transaction bodies.
//!
//! A block requested with `full_transactions=true` embeds complete
//! transaction objects, but those are cached separately from the block: the
//! same transaction data serves `eth_getTransactionByHash`, and the
//! block-only part (with hashes) serves `full_transactions=false` requests.
//!
//! - [`decompose`] takes a full-block response apart into a block-only
//!   [`BlockContainer`] plus the individual [`TxContainer`]s, for caching.
//! - [`reconstruct`] splices cached transaction JSONs back into a cached
//!   block-only JSON, producing the full-block response without touching the
//!   upstream.
//!
//! Both operate on the raw JSON text instead of re-serializing through
//! `serde_json::Value`, so everything outside the `transactions` array is
//! preserved byte-for-byte as the upstream returned it.

use crate::data::{BlockContainer, TxContainer, TxId};
use crate::upstream::ethereum::head::parse_eth_block;
use crate::upstream::ethereum::parse_hex_quantity;
use serde_json::value::RawValue;
use std::sync::Arc;

/// Split a full-block JSON into the block-only container (transactions
/// replaced by their hashes) and the individual transactions.
///
/// A block whose `transactions` array is empty or already contains hashes is
/// returned as-is with no transactions — it is its own block-only form.
pub fn decompose(raw_json: &str) -> Option<(BlockContainer, Vec<TxContainer>)> {
    let (open, close) = transactions_bounds(raw_json)?;
    let tx_values: Vec<&RawValue> = serde_json::from_str(&raw_json[open..=close]).ok()?;

    if tx_values.is_empty() || !tx_values[0].get().starts_with('{') {
        return parse_eth_block(raw_json).map(|block| (block, Vec::new()));
    }

    let mut txs = Vec::with_capacity(tx_values.len());
    let mut hashes = String::new();
    for value in tx_values {
        let tx = parse_eth_tx(value.get())?;
        if !hashes.is_empty() {
            hashes.push(',');
        }
        hashes.push('"');
        hashes.push_str(&tx.hash.to_hex_prefixed());
        hashes.push('"');
        txs.push(tx);
    }

    let block_only = format!("{}{}{}", &raw_json[..=open], hashes, &raw_json[close..]);
    let block = parse_eth_block(&block_only)?;
    Some((block, txs))
}

/// Splice the given transactions into a block-only JSON (with tx hashes),
/// producing the full-block representation.
///
/// The transactions must be in block order and each must carry its JSON;
/// returns `None` otherwise — the caller treats that as a cache miss.
pub fn reconstruct(block_json: &str, txs: &[TxContainer]) -> Option<String> {
    let (open, close) = transactions_bounds(block_json)?;

    let mut tx_jsons = Vec::with_capacity(txs.len());
    for tx in txs {
        tx_jsons.push(std::str::from_utf8(tx.json.as_deref()?).ok()?);
    }

    let joined = tx_jsons.join(",");
    let mut full = String::with_capacity(block_json.len() + joined.len());
    full.push_str(&block_json[..=open]);
    full.push_str(&joined);
    full.push_str(&block_json[close..]);
    Some(full)
}

/// Parse a transaction JSON (from `eth_getTransactionByHash` or a full-block
/// `transactions` element) into a [`TxContainer`], keeping the raw JSON.
pub fn parse_eth_tx(raw_json: &str) -> Option<TxContainer> {
    let v: serde_json::Value = serde_json::from_str(raw_json).ok()?;

    let hash: TxId = v.get("hash")?.as_str()?.parse().ok()?;
    let block_hash = v
        .get("blockHash")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok());
    let height = v
        .get("blockNumber")
        .and_then(|v| v.as_str())
        .and_then(parse_hex_quantity);

    Some(TxContainer {
        hash,
        block_hash,
        height,
        json: Some(Arc::from(raw_json.as_bytes())),
    })
}

/// Locate the value of the top-level `transactions` field, returning the
/// positions of its `[` and `]` brackets.
///
/// Transaction objects contain nested arrays (e.g. `accessList`) and strings
/// that may hold any characters, so this is a real scanner rather than a
/// substring search.
fn transactions_bounds(json: &str) -> Option<(usize, usize)> {
    let bytes = json.as_bytes();
    let mut depth = 0u32;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' if depth == 1 => {
                let start = i;
                let after = skip_string(bytes, i)?;
                i = after;
                let mut j = after;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                // A string not followed by ':' is a value, not a key
                if j >= bytes.len() || bytes[j] != b':' {
                    continue;
                }
                i = j + 1;
                if &json[start + 1..after - 1] != "transactions" {
                    continue;
                }
                let mut k = i;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                if k >= bytes.len() || bytes[k] != b'[' {
                    return None;
                }
                return Some((k, matching_bracket(bytes, k)?));
            }
            b'"' => i = skip_string(bytes, i)?,
            b'{' | b'[' => {
                depth += 1;
                i += 1;
            }
            b'}' | b']' => {
                depth = depth.checked_sub(1)?;
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

/// `i` points at an opening quote; returns the index just after the closing
/// quote.
fn skip_string(bytes: &[u8], i: usize) -> Option<usize> {
    let mut i = i + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i + 1),
            _ => i += 1,
        }
    }
    None
}

/// `open` points at a `[`; returns the index of the matching `]`.
fn matching_bracket(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0u32;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i = skip_string(bytes, i)?;
                continue;
            }
            b'[' | b'{' => depth += 1,
            b']' | b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return if bytes[i] == b']' { Some(i) } else { None };
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const TX_A: &str = r#"{"hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","blockHash":"0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75","blockNumber":"0x64","accessList":[{"address":"0x1","storageKeys":["0x2"]}],"input":"0x5b5d"}"#;
    const TX_B: &str = r#"{"hash":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","blockHash":"0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75","blockNumber":"0x64"}"#;

    fn full_block_json() -> String {
        format!(
            r#"{{"hash":"0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75","number":"0x64","parentHash":"0x787899711b862b77df8d2faa69de664048598265a9f96abf178d341076e200e0","timestamp":"0x65a8b44c","transactions":[{TX_A},{TX_B}],"gasUsed":"0x1234"}}"#
        )
    }

    #[test]
    fn decomposes_full_block() {
        let (block, txs) = decompose(&full_block_json()).unwrap();

        assert_eq!(block.height, 0x64);
        assert_eq!(block.transaction_hashes.len(), 2);
        assert_eq!(txs.len(), 2);
        assert_eq!(txs[0].hash.to_hex(), "a".repeat(64));
        assert_eq!(txs[0].height, Some(0x64));
        assert!(txs[0].block_hash.is_some());
        assert_eq!(std::str::from_utf8(txs[0].json.as_deref().unwrap()).unwrap(), TX_A);
        assert_eq!(std::str::from_utf8(txs[1].json.as_deref().unwrap()).unwrap(), TX_B);
    }

    #[test]
    fn decomposed_block_json_has_hashes() {
        let (block, _) = decompose(&full_block_json()).unwrap();

        let json = std::str::from_utf8(block.json.as_deref().unwrap()).unwrap();
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(
            v["transactions"],
            serde_json::json!([format!("0x{}", "a".repeat(64)), format!("0x{}", "b".repeat(64))])
        );
        // The rest of the block is preserved as the upstream sent it
        assert_eq!(v["gasUsed"], "0x1234");
        assert_eq!(v["timestamp"], "0x65a8b44c");
    }

    #[test]
    fn reconstruct_restores_original() {
        let original = full_block_json();
        let (block, txs) = decompose(&original).unwrap();

        let block_json = std::str::from_utf8(block.json.as_deref().unwrap()).unwrap();
        let full = reconstruct(block_json, &txs).unwrap();

        assert_eq!(full, original);
    }

    #[test]
    fn decompose_block_without_transactions() {
        let json = r#"{"hash":"0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75","number":"0x64","timestamp":"0x65a8b44c","transactions":[]}"#;

        let (block, txs) = decompose(json).unwrap();

        assert!(txs.is_empty());
        assert_eq!(std::str::from_utf8(block.json.as_deref().unwrap()).unwrap(), json);
    }

    #[test]
    fn decompose_block_with_hashes_only() {
        // Defensive: the upstream answered with tx hashes even though full
        // bodies were requested — there is nothing to split
        let json = r#"{"hash":"0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75","number":"0x64","timestamp":"0x65a8b44c","transactions":["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}"#;

        let (block, txs) = decompose(json).unwrap();

        assert!(txs.is_empty());
        assert_eq!(block.transaction_hashes.len(), 1);
    }

    #[test]
    fn reconstruct_empty_transactions_is_identity() {
        let json = r#"{"hash":"0x1","transactions":[],"gasUsed":"0x0"}"#;
        assert_eq!(reconstruct(json, &[]).unwrap(), json);
    }

    #[test]
    fn reconstruct_requires_tx_json() {
        let json = r#"{"hash":"0x1","transactions":["0xaa"]}"#;
        let tx = TxContainer {
            hash: TxId::from_bytes([0xaa; 32]),
            block_hash: None,
            height: None,
            json: None,
        };
        assert!(reconstruct(json, &[tx]).is_none());
    }

    #[test]
    fn parse_tx_pending_has_no_block() {
        let json = r#"{"hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","blockHash":null,"blockNumber":null}"#;
        let tx = parse_eth_tx(json).unwrap();
        assert!(tx.block_hash.is_none());
        assert!(tx.height.is_none());
    }

    #[test]
    fn parse_tx_rejects_invalid() {
        assert!(parse_eth_tx("not json").is_none());
        assert!(parse_eth_tx(r#"{"blockHash":"0x1"}"#).is_none()); // missing hash
    }

    // ── transactions_bounds scanner ─────────────────────────────────────

    fn bounds_content(json: &str) -> Option<String> {
        transactions_bounds(json).map(|(open, close)| json[open..=close].to_string())
    }

    #[test]
    fn bounds_finds_simple_array() {
        let json = r#"{"a":1,"transactions":["0x1","0x2"],"b":2}"#;
        assert_eq!(bounds_content(json).unwrap(), r#"["0x1","0x2"]"#);
    }

    #[test]
    fn bounds_skips_nested_brackets_in_tx_objects() {
        let json = r#"{"transactions":[{"accessList":[["0x[",  "]"]],"x":1}],"b":[]}"#;
        assert_eq!(
            bounds_content(json).unwrap(),
            r#"[{"accessList":[["0x[",  "]"]],"x":1}]"#
        );
    }

    #[test]
    fn bounds_ignores_string_values_mentioning_transactions() {
        let json = r#"{"extraData":"transactions","transactions":["0x1"]}"#;
        assert_eq!(bounds_content(json).unwrap(), r#"["0x1"]"#);
    }

    #[test]
    fn bounds_ignores_nested_transactions_key() {
        let json = r#"{"inner":{"transactions":["0xdeep"]},"transactions":["0x1"]}"#;
        assert_eq!(bounds_content(json).unwrap(), r#"["0x1"]"#);
    }

    #[test]
    fn bounds_handles_escaped_quotes() {
        let json = r#"{"extraData":"say \"transactions\": [","transactions":["0x1"]}"#;
        assert_eq!(bounds_content(json).unwrap(), r#"["0x1"]"#);
    }

    #[test]
    fn bounds_none_without_transactions() {
        assert!(transactions_bounds(r#"{"a":1}"#).is_none());
        assert!(transactions_bounds("null").is_none());
        assert!(transactions_bounds("not json at all").is_none());
    }

    #[test]
    fn bounds_none_when_transactions_not_array() {
        assert!(transactions_bounds(r#"{"transactions":null}"#).is_none());
    }
}
