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

//! Parsing of human-readable byte size strings (e.g., "5mb", "1kb", "1024").

use serde::{Deserialize, Deserializer};

/// Parses a byte size string into a number of bytes.
///
/// Supports plain numbers (interpreted as bytes) and suffixed values:
/// `b`, `kb`, `mb`, `gb` (case-insensitive).
pub fn parse_bytes(input: &str) -> Result<usize, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("empty byte size string".to_string());
    }

    // Split into numeric and unit parts
    let (num_str, unit) = split_number_unit(input);
    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid number in byte size: {input}"))?;

    let multiplier = match unit.to_lowercase().as_str() {
        "" | "b" => 1,
        "kb" | "k" => 1024,
        "mb" | "m" => 1024 * 1024,
        "gb" | "g" => 1024 * 1024 * 1024,
        other => return Err(format!("unknown byte size unit: {other}")),
    };

    Ok((num * multiplier as f64) as usize)
}

fn split_number_unit(s: &str) -> (&str, &str) {
    let unit_start = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    (&s[..unit_start], &s[unit_start..])
}

/// Serde deserializer for optional byte size fields.
/// Accepts either a number (bytes) or a string with unit suffix.
pub fn deserialize_opt_bytes<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_yaml::Value>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(serde_yaml::Value::Number(n)) => n
            .as_u64()
            .map(|v| Some(v as usize))
            .ok_or_else(|| serde::de::Error::custom("invalid byte size number")),
        Some(serde_yaml::Value::String(s)) => {
            parse_bytes(&s).map(Some).map_err(serde::de::Error::custom)
        }
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected number or string for byte size, got: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plain_number() {
        assert_eq!(parse_bytes("1024").unwrap(), 1024);
    }

    #[test]
    fn parse_bytes_suffix() {
        assert_eq!(parse_bytes("100b").unwrap(), 100);
    }

    #[test]
    fn parse_kb() {
        assert_eq!(parse_bytes("1kb").unwrap(), 1024);
        assert_eq!(parse_bytes("1k").unwrap(), 1024);
        assert_eq!(parse_bytes("1K").unwrap(), 1024);
        assert_eq!(parse_bytes("2KB").unwrap(), 2048);
        assert_eq!(parse_bytes("16kb").unwrap(), 16 * 1024);
    }

    #[test]
    fn parse_mb() {
        assert_eq!(parse_bytes("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes("1mb").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes("4mb").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_bytes("5MB").unwrap(), 5 * 1024 * 1024);
        assert_eq!(parse_bytes("10Mb").unwrap(), 10 * 1024 * 1024);
    }

    #[test]
    fn parse_gb() {
        assert_eq!(parse_bytes("1gb").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn rejects_unknown_unit() {
        assert!(parse_bytes("5tb").is_err());
    }

    #[test]
    fn rejects_empty() {
        assert!(parse_bytes("").is_err());
    }
}
