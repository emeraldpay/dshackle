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

//! Blockchain identification types used throughout the application.

use emerald_api::proto::common::ChainRef;
use std::fmt;
use std::str::FromStr;

/// Identifies a target blockchain for routing and upstream selection.
///
/// Currently only wraps a standard protobuf `ChainRef`, but designed as an
/// enum so we can later add custom (non-protobuf) blockchain definitions
/// without changing the rest of the codebase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TargetBlockchain {
    /// A blockchain known to the emerald-api protobuf schema.
    Standard(ChainRef),
}

/// Broad protocol family of a blockchain. Used where behavior depends on the
/// protocol (subscriptions, status thresholds, default method sets) rather than
/// on the specific chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockchainType {
    Bitcoin,
    Ethereum,
    /// A chain the rewrite doesn't classify yet (e.g. a non-standard id).
    Unknown,
}

impl TargetBlockchain {
    /// Short code for display and logging (e.g. "ETH", "BTC").
    pub fn code(&self) -> String {
        match self {
            TargetBlockchain::Standard(chain) => chain.code(),
        }
    }

    /// Full chain name as the legacy implementation printed it — the constant
    /// name of the Java `io.emeraldpay.api.Chain` enum. Used in outputs that
    /// must stay text-compatible with legacy, e.g. the health check.
    pub fn legacy_name(&self) -> &'static str {
        match self {
            TargetBlockchain::Standard(chain) => match chain {
                ChainRef::ChainBitcoin => "BITCOIN",
                ChainRef::ChainEthereum => "ETHEREUM",
                ChainRef::ChainEthereumClassic => "ETHEREUM_CLASSIC",
                ChainRef::ChainFantom => "FANTOM",
                ChainRef::ChainMatic => "MATIC",
                ChainRef::ChainRsk => "RSK",
                ChainRef::ChainMorden => "TESTNET_MORDEN",
                ChainRef::ChainKovan => "TESTNET_KOVAN",
                ChainRef::ChainTestnetBitcoin => "TESTNET_BITCOIN",
                ChainRef::ChainTestnetBitcoin4 => "TESTNET_BITCOIN_V4",
                ChainRef::ChainGoerli => "TESTNET_GOERLI",
                ChainRef::ChainRopsten => "TESTNET_ROPSTEN",
                ChainRef::ChainRinkeby => "TESTNET_RINKEBY",
                ChainRef::ChainHolesky => "TESTNET_HOLESKY",
                ChainRef::ChainSepolia => "TESTNET_SEPOLIA",
                ChainRef::ChainHoodi => "TESTNET_HOODI",
                _ => "UNSPECIFIED",
            },
        }
    }

    /// Classify the chain into its protocol family.
    pub fn blockchain_type(&self) -> BlockchainType {
        match self {
            TargetBlockchain::Standard(chain) => match chain {
                ChainRef::ChainBitcoin
                | ChainRef::ChainTestnetBitcoin
                | ChainRef::ChainTestnetBitcoin4 => BlockchainType::Bitcoin,
                ChainRef::ChainEthereum
                | ChainRef::ChainEthereumClassic
                | ChainRef::ChainFantom
                | ChainRef::ChainMatic
                | ChainRef::ChainRsk
                | ChainRef::ChainMorden
                | ChainRef::ChainKovan
                | ChainRef::ChainGoerli
                | ChainRef::ChainRopsten
                | ChainRef::ChainRinkeby
                | ChainRef::ChainHolesky
                | ChainRef::ChainSepolia
                | ChainRef::ChainHoodi => BlockchainType::Ethereum,
                _ => BlockchainType::Unknown,
            },
        }
    }

    /// Numeric chain id, shared with other Dshackle instances (JVM or Rust)
    /// in cross-instance data such as Redis cache keys.
    pub fn id(&self) -> i32 {
        match self {
            TargetBlockchain::Standard(chain) => *chain as i32,
        }
    }
}

impl From<ChainRef> for TargetBlockchain {
    fn from(chain: ChainRef) -> Self {
        TargetBlockchain::Standard(chain)
    }
}

impl fmt::Display for TargetBlockchain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TargetBlockchain::Standard(chain) => write!(f, "{}", chain.code()),
        }
    }
}

impl FromStr for TargetBlockchain {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ChainRef::from_str(s)
            .map(TargetBlockchain::Standard)
            .map_err(|_| format!("unknown blockchain: {s}"))
    }
}

/// Tries to convert a protobuf `i32` chain value into a `TargetBlockchain`.
/// Returns `None` for unknown or unspecified chain values.
impl TryFrom<i32> for TargetBlockchain {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        ChainRef::try_from(value)
            .map(TargetBlockchain::Standard)
            .map_err(|_| value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_chain_ref() {
        let target: TargetBlockchain = ChainRef::ChainEthereum.into();
        assert_eq!(target, TargetBlockchain::Standard(ChainRef::ChainEthereum));
        assert_eq!(target.code(), "ETH");
        assert_eq!(target.to_string(), "ETH");
    }

    #[test]
    fn blockchain_type_classifies_families() {
        let eth: TargetBlockchain = ChainRef::ChainEthereum.into();
        let etc: TargetBlockchain = ChainRef::ChainEthereumClassic.into();
        let btc: TargetBlockchain = ChainRef::ChainBitcoin.into();
        let tbtc: TargetBlockchain = ChainRef::ChainTestnetBitcoin4.into();
        assert_eq!(eth.blockchain_type(), BlockchainType::Ethereum);
        assert_eq!(etc.blockchain_type(), BlockchainType::Ethereum);
        assert_eq!(btc.blockchain_type(), BlockchainType::Bitcoin);
        assert_eq!(tbtc.blockchain_type(), BlockchainType::Bitcoin);
    }

    #[test]
    fn try_from_valid_i32() {
        let target = TargetBlockchain::try_from(100).unwrap();
        assert_eq!(target, TargetBlockchain::Standard(ChainRef::ChainEthereum));
    }

    #[test]
    fn parse_from_str() {
        let target: TargetBlockchain = "ethereum".parse().unwrap();
        assert_eq!(target, TargetBlockchain::Standard(ChainRef::ChainEthereum));

        let target: TargetBlockchain = "btc".parse().unwrap();
        assert_eq!(target, TargetBlockchain::Standard(ChainRef::ChainBitcoin));

        let err = "not_a_chain".parse::<TargetBlockchain>().unwrap_err();
        assert!(err.contains("unknown blockchain"));
    }

    #[test]
    fn try_from_invalid_i32() {
        let err = TargetBlockchain::try_from(999999).unwrap_err();
        assert_eq!(err, 999999);
    }

    #[test]
    fn eq_and_hash() {
        use std::collections::HashSet;
        let a: TargetBlockchain = ChainRef::ChainEthereum.into();
        let b: TargetBlockchain = ChainRef::ChainEthereum.into();
        let c: TargetBlockchain = ChainRef::ChainBitcoin.into();

        assert_eq!(a, b);
        assert_ne!(a, c);

        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b);
        set.insert(c);
        assert_eq!(set.len(), 2);
    }
}
