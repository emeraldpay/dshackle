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

use serde::Deserialize;

/// A configured token to track.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenConfig {
    pub id: String,
    pub blockchain: String,
    pub name: String,
    #[serde(rename = "type")]
    pub token_type: TokenType,
    pub address: String,
}

/// Supported token standards.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum TokenType {
    #[serde(rename = "ERC-20", alias = "erc-20", alias = "ERC20", alias = "erc20")]
    Erc20,
}
