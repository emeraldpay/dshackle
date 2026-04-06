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

//! Redis cache configuration.

use serde::Deserialize;

/// Top-level cache configuration wrapper.
#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    pub redis: Option<RedisConfig>,
}

/// Redis connection configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RedisConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub db: u8,
    pub password: Option<String>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            password: None,
        }
    }
}

impl CacheConfig {
    /// Returns the Redis config only if it is present and enabled.
    pub fn redis(&self) -> Option<&RedisConfig> {
        self.redis.as_ref().filter(|r| r.enabled)
    }
}
