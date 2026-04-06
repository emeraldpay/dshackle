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

//! Prometheus monitoring configuration.

use serde::Deserialize;

/// Monitoring endpoint configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MonitoringConfig {
    pub enabled: bool,
    pub extended: bool,
    pub prometheus: PrometheusConfig,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            extended: false,
            prometheus: PrometheusConfig::default(),
        }
    }
}

/// Prometheus-specific monitoring settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PrometheusConfig {
    pub enabled: bool,
    /// Host to bind the metrics endpoint.
    #[serde(alias = "host")]
    pub bind: String,
    pub port: u16,
    pub path: String,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: "127.0.0.1".to_string(),
            port: 8081,
            path: "/metrics".to_string(),
        }
    }
}
