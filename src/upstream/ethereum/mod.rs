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

//! Ethereum-specific upstream implementations.

pub mod head;
pub mod http;
pub mod validator;
mod ws_conn;
mod ws_pool;
mod ws_single;

pub use ws_conn::WsTarget;
pub use ws_single::EthereumWsUpstream;

/// Parse an Ethereum-style hex quantity (`"0x10d4f"`, `0x10d4f`, `"0x"` for
/// zero) into a `u64`. Tolerates surrounding double quotes so callers can
/// pass either a Rust string or the raw JSON text from a `RawValue`.
///
/// Returns `None` if the input is missing the `0x` prefix or contains
/// non-hex characters.
pub fn parse_hex_quantity(s: &str) -> Option<u64> {
    let s = s.trim().trim_matches('"');
    let hex = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"))?;
    if hex.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(hex, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_block_number() {
        assert_eq!(parse_hex_quantity(r#""0x10d4f""#), Some(0x10d4f));
    }

    #[test]
    fn parses_zero() {
        assert_eq!(parse_hex_quantity(r#""0x0""#), Some(0));
        assert_eq!(parse_hex_quantity("0x"), Some(0));
    }

    #[test]
    fn parses_large_value() {
        assert_eq!(parse_hex_quantity(r#""0x1312d00""#), Some(20_000_000));
    }

    #[test]
    fn parses_without_quotes() {
        assert_eq!(parse_hex_quantity("0x10d4f"), Some(0x10d4f));
    }

    #[test]
    fn parses_uppercase_prefix() {
        assert_eq!(parse_hex_quantity("0XFF"), Some(255));
    }

    #[test]
    fn rejects_missing_prefix() {
        assert_eq!(parse_hex_quantity("1a"), None);
    }

    #[test]
    fn rejects_non_hex() {
        assert_eq!(parse_hex_quantity(r#""not_hex""#), None);
        assert_eq!(parse_hex_quantity("hello"), None);
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(parse_hex_quantity(r#""""#), None);
        assert_eq!(parse_hex_quantity(""), None);
    }
}
