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

//! Access log and request log configuration.

use serde::Deserialize;

/// Configuration for logging client requests (egress / access log).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AccessLogConfig {
    pub enabled: bool,
    #[serde(alias = "includeMessages", rename = "include-messages")]
    pub include_messages: bool,
    #[serde(flatten)]
    pub target: LogTargetConfig,
}

impl Default for AccessLogConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            include_messages: false,
            target: LogTargetConfig::file("./access_log.jsonl"),
        }
    }
}

/// Configuration for logging upstream requests (ingress / request log).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RequestLogConfig {
    pub enabled: bool,
    #[serde(alias = "includeParams", rename = "include-params")]
    pub include_params: bool,
    #[serde(flatten)]
    pub target: LogTargetConfig,
}

impl Default for RequestLogConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            include_params: false,
            target: LogTargetConfig::file("./request_log.jsonl"),
        }
    }
}

/// Log output target configuration (file or socket).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogTargetConfig {
    #[serde(rename = "type", default = "default_log_type")]
    pub target_type: String,
    pub filename: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub encoding: Option<String>,
    #[serde(alias = "max-buffer", alias = "max_buffer")]
    pub buffer: Option<usize>,
}

fn default_log_type() -> String {
    "file".to_string()
}

impl Default for LogTargetConfig {
    fn default() -> Self {
        Self {
            target_type: "file".to_string(),
            filename: None,
            host: None,
            port: None,
            encoding: None,
            buffer: None,
        }
    }
}

impl LogTargetConfig {
    pub fn file(path: &str) -> Self {
        Self {
            target_type: "file".to_string(),
            filename: Some(path.to_string()),
            ..Default::default()
        }
    }

    /// Returns the resolved log encoding for socket targets.
    pub fn resolved_encoding(&self) -> LogEncoding {
        self.encoding
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or_default()
    }
}

/// Encoding format for socket-based log targets.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LogEncoding {
    /// Append a newline after each log event.
    NewLine,
    /// Prepend a 32-bit length prefix to each log event.
    #[default]
    SizePrefix,
}

impl std::str::FromStr for LogEncoding {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "nl" | "newline" | "new-line" | "new_line" => Ok(LogEncoding::NewLine),
            "prefix" | "length" | "length_prefix" | "length-prefix" => Ok(LogEncoding::SizePrefix),
            other => Err(format!("unknown log encoding: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── File target ──────────────────────────────────────────────────────

    #[test]
    fn parse_file_target_basic() {
        let yaml = "type: file\n";
        let cfg: LogTargetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.target_type, "file");
        assert!(cfg.filename.is_none());
    }

    #[test]
    fn parse_file_target_with_path() {
        let yaml = "type: file\nfilename: /var/log/hello.log\n";
        let cfg: LogTargetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.target_type, "file");
        assert_eq!(cfg.filename.as_deref(), Some("/var/log/hello.log"));
    }

    #[test]
    fn parse_file_target_path_only() {
        let yaml = "filename: /var/log/hello.log\n";
        let cfg: LogTargetConfig = serde_yaml::from_str(yaml).unwrap();
        // defaults to "file" type
        assert_eq!(cfg.target_type, "file");
        assert_eq!(cfg.filename.as_deref(), Some("/var/log/hello.log"));
    }

    // ── Socket target ────────────────────────────────────────────────────

    #[test]
    fn parse_socket_target_basic() {
        let yaml = "type: socket\nport: 9000\n";
        let cfg: LogTargetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.target_type, "socket");
        assert_eq!(cfg.port, Some(9000));
        assert!(cfg.host.is_none());
        // default encoding
        assert_eq!(cfg.resolved_encoding(), LogEncoding::SizePrefix);
    }

    #[test]
    fn parse_socket_target_custom_host() {
        let yaml = "type: socket\nport: 9000\nhost: 10.0.5.20\n";
        let cfg: LogTargetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.host.as_deref(), Some("10.0.5.20"));
    }

    #[test]
    fn parse_socket_newline_encoding() {
        let yaml = "type: socket\nport: 9000\nencoding: new-line\n";
        let cfg: LogTargetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.resolved_encoding(), LogEncoding::NewLine);
    }

    #[test]
    fn parse_socket_length_prefix_encoding() {
        let yaml = "type: socket\nport: 9000\nencoding: length-prefix\n";
        let cfg: LogTargetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.resolved_encoding(), LogEncoding::SizePrefix);
    }

    // ── LogEncoding FromStr ──────────────────────────────────────────────

    #[test]
    fn log_encoding_from_str() {
        assert_eq!("nl".parse::<LogEncoding>().unwrap(), LogEncoding::NewLine);
        assert_eq!(
            "newline".parse::<LogEncoding>().unwrap(),
            LogEncoding::NewLine
        );
        assert_eq!(
            "new-line".parse::<LogEncoding>().unwrap(),
            LogEncoding::NewLine
        );
        assert_eq!(
            "new_line".parse::<LogEncoding>().unwrap(),
            LogEncoding::NewLine
        );
        assert_eq!(
            "prefix".parse::<LogEncoding>().unwrap(),
            LogEncoding::SizePrefix
        );
        assert_eq!(
            "length".parse::<LogEncoding>().unwrap(),
            LogEncoding::SizePrefix
        );
        assert_eq!(
            "length-prefix".parse::<LogEncoding>().unwrap(),
            LogEncoding::SizePrefix
        );
        assert_eq!(
            "length_prefix".parse::<LogEncoding>().unwrap(),
            LogEncoding::SizePrefix
        );
        assert!("unknown".parse::<LogEncoding>().is_err());
    }
}
