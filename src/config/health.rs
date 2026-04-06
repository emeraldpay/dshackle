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

//! Health check endpoint configuration.

use serde::Deserialize;

/// Health check endpoint configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    pub port: u16,
    pub host: String,
    pub path: String,
    #[serde(default)]
    pub blockchains: Vec<ChainHealthConfig>,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            port: 8082,
            host: "127.0.0.1".to_string(),
            path: "/health".to_string(),
            blockchains: Vec::new(),
        }
    }
}

impl HealthConfig {
    pub fn is_enabled(&self) -> bool {
        !self.blockchains.is_empty()
    }
}

/// Per-blockchain health check requirements.
#[derive(Debug, Clone, Deserialize)]
pub struct ChainHealthConfig {
    /// Blockchain identifier (accepts both `blockchain` and `chain` YAML keys).
    #[serde(alias = "chain")]
    pub blockchain: String,
    /// Minimum number of available upstreams to consider healthy.
    #[serde(
        default = "default_min_available",
        alias = "minAvailable",
        alias = "min-availability",
        alias = "minAvailability"
    )]
    #[serde(rename = "min-available")]
    pub min_available: u32,
}

fn default_min_available() -> u32 {
    1
}
