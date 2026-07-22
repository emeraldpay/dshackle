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

//! Token configuration for tracking ERC-20 balances.

use crate::blockchain::{BlockchainType, TargetBlockchain};
use crate::upstream::balance::is_valid_eth_address;
use serde::Deserialize;
use std::fmt;
use std::str::FromStr;
use tracing;

/// A configured token to track, with every field already validated.
///
/// The config's `id` field is not carried over: like in legacy, it only has to
/// be present and non-blank, and it names the token in the validation error
/// log — nothing reads it at runtime.
#[derive(Debug, Clone)]
pub struct TokenConfig {
    pub blockchain: TargetBlockchain,
    /// Coin name, the key a `GetBalance` request uses to select the token.
    pub name: String,
    pub token_type: TokenType,
    /// Contract address of the token.
    pub address: String,
}

/// Supported token standards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    Erc20,
}

/// The `type` value is not a supported token standard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownTokenType(pub String);

impl FromStr for TokenType {
    type Err = UnknownTokenType;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Legacy compared the uppercased value to "ERC-20"; the dash-less
        // spelling was always accepted by this rewrite, so keep both.
        match s.to_ascii_uppercase().as_str() {
            "ERC-20" | "ERC20" => Ok(TokenType::Erc20),
            _ => Err(UnknownTokenType(s.to_string())),
        }
    }
}

/// A token entry as it appears in the YAML, before validation. Every field is
/// optional at this stage because a broken token entry must not fail the whole
/// config: legacy dropped it with an error log, and we keep that behavior.
#[derive(Debug, Deserialize)]
pub struct RawTokenConfig {
    id: Option<String>,
    blockchain: Option<String>,
    name: Option<String>,
    #[serde(rename = "type")]
    token_type: Option<String>,
    address: Option<String>,
}

/// Name of the token config field that failed validation, for the error log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidTokenField {
    Id,
    Blockchain,
    Name,
    Type,
    Address,
}

impl fmt::Display for InvalidTokenField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Printed exactly as the YAML key is spelled, matching the legacy log.
        let name = match self {
            InvalidTokenField::Id => "id",
            InvalidTokenField::Blockchain => "blockchain",
            InvalidTokenField::Name => "name",
            InvalidTokenField::Type => "type",
            InvalidTokenField::Address => "address",
        };
        write!(f, "{name}")
    }
}

impl RawTokenConfig {
    /// Checks the fields in the same order as legacy `TokensConfig.Token.validate`,
    /// reporting the first missing or malformed one.
    fn validate(self) -> Result<TokenConfig, InvalidTokenField> {
        // Parsed before the field-order checks because legacy warned about an
        // unrecognized type at read time, even when an earlier field is also
        // invalid and names the validation error.
        let token_type = self.token_type.and_then(|s| match s.parse::<TokenType>() {
            Ok(t) => Some(t),
            Err(UnknownTokenType(s)) => {
                tracing::warn!("Invalid token type: {}", s);
                None
            }
        });

        self.id
            .filter(|s| !s.trim().is_empty())
            .ok_or(InvalidTokenField::Id)?;
        let blockchain = self
            .blockchain
            .and_then(|s| s.parse::<TargetBlockchain>().ok())
            .ok_or(InvalidTokenField::Blockchain)?;
        let name = self
            .name
            .filter(|s| !s.trim().is_empty())
            .ok_or(InvalidTokenField::Name)?;
        let token_type = token_type.ok_or(InvalidTokenField::Type)?;
        let address = self
            .address
            .filter(|s| !s.trim().is_empty())
            .ok_or(InvalidTokenField::Address)?;
        if blockchain.blockchain_type() == BlockchainType::Ethereum
            && !is_valid_eth_address(&address)
        {
            return Err(InvalidTokenField::Address);
        }

        Ok(TokenConfig {
            blockchain,
            name,
            token_type,
            address,
        })
    }
}

/// Validates the configured tokens, dropping each invalid entry with an error
/// log naming the bad field — same as the legacy `TokensConfigReader`, where a
/// broken token never failed the rest of the config.
pub fn validate_tokens(raw: Vec<RawTokenConfig>) -> Vec<TokenConfig> {
    raw.into_iter()
        .filter_map(|token| {
            // Legacy printed the id through a Kotlin string template, so a
            // missing id showed up as "null" — keep the log identical.
            let id = token.id.clone().unwrap_or_else(|| "null".to_string());
            match token.validate() {
                Ok(valid) => Some(valid),
                Err(field) => {
                    tracing::error!("Failed to parse token {}. Invalid field: {}", id, field);
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use emerald_api::proto::common::ChainRef;

    fn raw(
        id: Option<&str>,
        blockchain: Option<&str>,
        name: Option<&str>,
        token_type: Option<&str>,
        address: Option<&str>,
    ) -> RawTokenConfig {
        RawTokenConfig {
            id: id.map(str::to_string),
            blockchain: blockchain.map(str::to_string),
            name: name.map(str::to_string),
            token_type: token_type.map(str::to_string),
            address: address.map(str::to_string),
        }
    }

    const DAI: &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";

    #[test]
    fn valid_token_passes() {
        let tokens = validate_tokens(vec![raw(
            Some("dai"),
            Some("ethereum"),
            Some("DAI"),
            Some("ERC-20"),
            Some(DAI),
        )]);
        assert_eq!(tokens.len(), 1);
        assert_eq!(
            tokens[0].blockchain,
            TargetBlockchain::Standard(ChainRef::ChainEthereum)
        );
        assert_eq!(tokens[0].name, "DAI");
        assert_eq!(tokens[0].token_type, TokenType::Erc20);
        assert_eq!(tokens[0].address, DAI);
    }

    #[test]
    fn token_type_spellings() {
        for spelling in ["ERC-20", "erc-20", "Erc-20", "ERC20", "erc20"] {
            assert_eq!(spelling.parse::<TokenType>(), Ok(TokenType::Erc20));
        }
        assert_eq!(
            "ERC-721".parse::<TokenType>(),
            Err(UnknownTokenType("ERC-721".to_string()))
        );
    }

    #[test]
    fn missing_or_blank_field_drops_token() {
        let cases = [
            raw(
                None,
                Some("ethereum"),
                Some("DAI"),
                Some("ERC-20"),
                Some(DAI),
            ),
            raw(
                Some(" "),
                Some("ethereum"),
                Some("DAI"),
                Some("ERC-20"),
                Some(DAI),
            ),
            raw(Some("dai"), None, Some("DAI"), Some("ERC-20"), Some(DAI)),
            raw(
                Some("dai"),
                Some("ethereum"),
                None,
                Some("ERC-20"),
                Some(DAI),
            ),
            raw(Some("dai"), Some("ethereum"), Some("DAI"), None, Some(DAI)),
            raw(
                Some("dai"),
                Some("ethereum"),
                Some("DAI"),
                Some("ERC-20"),
                None,
            ),
        ];
        for case in cases {
            assert!(validate_tokens(vec![case]).is_empty());
        }
    }

    #[test]
    fn unknown_blockchain_drops_token() {
        let tokens = validate_tokens(vec![raw(
            Some("dai"),
            Some("not_a_chain"),
            Some("DAI"),
            Some("ERC-20"),
            Some(DAI),
        )]);
        assert!(tokens.is_empty());
    }

    #[test]
    fn unknown_type_drops_token() {
        let tokens = validate_tokens(vec![raw(
            Some("punk"),
            Some("ethereum"),
            Some("Punk"),
            Some("ERC-721"),
            Some(DAI),
        )]);
        assert!(tokens.is_empty());
    }

    #[test]
    fn malformed_eth_address_drops_token() {
        let tokens = validate_tokens(vec![raw(
            Some("dai"),
            Some("ethereum"),
            Some("DAI"),
            Some("ERC-20"),
            Some("0x1234"),
        )]);
        assert!(tokens.is_empty());
    }

    #[test]
    fn invalid_token_does_not_affect_others() {
        let tokens = validate_tokens(vec![
            raw(
                None,
                Some("ethereum"),
                Some("BAD"),
                Some("ERC-20"),
                Some(DAI),
            ),
            raw(
                Some("dai"),
                Some("ethereum"),
                Some("DAI"),
                Some("ERC-20"),
                Some(DAI),
            ),
        ]);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].name, "DAI");
    }
}
