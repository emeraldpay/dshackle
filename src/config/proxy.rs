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

//! HTTP/WebSocket JSON RPC proxy configuration.

use crate::config::tls::ServerTlsConfig;
use serde::Deserialize;

/// HTTP proxy configuration for JSON RPC access.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ProxyConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    #[serde(alias = "websocket")]
    pub websocket: bool,
    pub tls: Option<ServerTlsConfig>,
    #[serde(default)]
    pub routes: Vec<Route>,
    #[serde(alias = "preserveBatchOrder", rename = "preserve-batch-order", default)]
    pub preserve_batch_order: bool,
    #[serde(alias = "corsOrigin", rename = "cors-origin")]
    pub cors_origin: Option<String>,
    #[serde(alias = "corsAllowedHeaders", rename = "cors-allowed-headers")]
    pub cors_allowed_headers: Option<String>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 8080,
            websocket: true,
            tls: None,
            routes: Vec::new(),
            preserve_batch_order: false,
            cors_origin: None,
            cors_allowed_headers: None,
        }
    }
}

/// A proxy route mapping a URL path to a blockchain.
#[derive(Debug, Clone, Deserialize)]
pub struct Route {
    pub id: String,
    pub blockchain: String,
}
