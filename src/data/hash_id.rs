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

//! 256-bit hash identifiers for blocks and transactions.
//!
//! Both [`BlockId`] and [`TxId`] wrap the same 32-byte array but are distinct
//! types so the compiler prevents mixing them up.

use std::fmt;
use std::str::FromStr;

/// Generates a 32-byte hash newtype with hex parsing, display, and standard
/// trait impls. Each invocation produces a distinct type so `BlockId` and `TxId`
/// cannot be accidentally interchanged.
macro_rules! hash_id {
    (
        $(#[$meta:meta])*
        $name:ident
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            pub fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            pub fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }

            /// Lowercase hex without `0x` prefix.
            pub fn to_hex(&self) -> String {
                hex::encode(self.0)
            }

            /// Lowercase hex with `0x` prefix.
            pub fn to_hex_prefixed(&self) -> String {
                format!("0x{}", hex::encode(self.0))
            }
        }

        impl FromStr for $name {
            type Err = hex::FromHexError;

            /// Parses a 64-character hex string, with or without a `0x` prefix.
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let s = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
                let bytes = hex::decode(s)?;
                let arr: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| hex::FromHexError::InvalidStringLength)?;
                Ok(Self(arr))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "0x{}", hex::encode(self.0))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                // Show abbreviated hash for readability in debug output
                let full = hex::encode(self.0);
                write!(f, "{}(0x{}..{})", stringify!($name), &full[..6], &full[58..])
            }
        }
    };
}

hash_id!(
    /// A 256-bit block hash.
    BlockId
);

hash_id!(
    /// A 256-bit transaction hash.
    TxId
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hex_without_prefix() {
        let hex = "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";
        let id: BlockId = hex.parse().unwrap();
        assert_eq!(id.to_hex(), hex);
    }

    #[test]
    fn parse_hex_with_prefix() {
        let hex = "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";
        let id: BlockId = hex.parse().unwrap();
        assert_eq!(id.to_hex_prefixed(), hex);
    }

    #[test]
    fn parse_uppercase_prefix() {
        let hex = "0Xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";
        let id: BlockId = hex.parse().unwrap();
        assert_eq!(&id.to_hex()[..4], "fc58");
    }

    #[test]
    fn rejects_wrong_length() {
        assert!("abcdef".parse::<BlockId>().is_err());
    }

    #[test]
    fn rejects_invalid_hex() {
        let bad = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        assert!(bad.parse::<BlockId>().is_err());
    }

    #[test]
    fn display_includes_prefix() {
        let hex = "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";
        let id: BlockId = hex.parse().unwrap();
        assert_eq!(format!("{id}"), format!("0x{hex}"));
    }

    #[test]
    fn debug_is_abbreviated() {
        let hex = "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";
        let id: BlockId = hex.parse().unwrap();
        let dbg = format!("{id:?}");
        assert!(dbg.starts_with("BlockId(0x"));
        assert!(dbg.contains(".."));
    }

    #[test]
    fn block_id_and_tx_id_are_distinct_types() {
        let bytes = [0u8; 32];
        let _block = BlockId::from_bytes(bytes);
        let _tx = TxId::from_bytes(bytes);
        // They wrap the same bytes but are different types — this is a
        // compile-time guarantee, not a runtime test.
    }

    #[test]
    fn equality_by_content() {
        let a: BlockId = "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75"
            .parse()
            .unwrap();
        let b: BlockId = "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75"
            .parse()
            .unwrap();
        assert_eq!(a, b);
    }
}
