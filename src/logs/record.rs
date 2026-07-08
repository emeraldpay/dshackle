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

//! Pieces shared by the access-log and request-log records.

use serde::{Serialize, Serializer};
use uuid::Uuid;

/// A timestamp serialized as ISO-8601 UTC, like the legacy logs. The exact
/// fractional-second rendering is jiff's (trailing zeros trimmed), not the
/// legacy 3-digit grouping — any RFC-3339 consumer reads both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogTimestamp(pub jiff::Timestamp);

impl LogTimestamp {
    /// The current moment, truncated to microseconds — the precision the
    /// legacy logs carried; full nanoseconds would only add noise.
    pub fn now() -> Self {
        let now = jiff::Timestamp::now();
        Self(jiff::Timestamp::from_microsecond(now.as_microsecond()).unwrap_or(now))
    }
}

impl std::fmt::Display for LogTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for LogTimestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// Which server carried the request, as the legacy `monitoring.Channel`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Channel {
    /// The native gRPC API.
    #[serde(rename = "DSHACKLE")]
    Dshackle,
    /// The JSON-RPC HTTP proxy.
    #[serde(rename = "JSONRPC")]
    JsonRpc,
    /// The JSON-RPC WebSocket proxy.
    #[serde(rename = "WSJSONRPC")]
    WsJsonRpc,
}

/// Who made the request to Dshackle.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Remote {
    /// Every address on the request path: forwarding headers plus the peer.
    pub ips: Vec<String>,
    /// The best guess of the actual client address (public addresses win over
    /// local ones).
    pub ip: String,
    pub user_agent: String,
}

/// The initial client request an event belongs to. Its id ties together all
/// records produced for one request (e.g. every reply of a subscription).
#[derive(Clone, Debug, Serialize)]
pub struct RequestDetails {
    pub id: Uuid,
    pub start: LogTimestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote: Option<Remote>,
}

impl Remote {
    /// Build from the transport peer address and the forwarding/user-agent
    /// headers, replicating the legacy `RecordBuilder` extraction: all
    /// addresses collected, the "best" (most public) one picked as `ip`, and
    /// the user agent cleaned and truncated to 128 characters.
    pub fn from_request(
        peer: Option<std::net::IpAddr>,
        x_real_ip: Option<&str>,
        x_forwarded_for: Option<&str>,
        user_agent: Option<&str>,
    ) -> Self {
        let mut ips: Vec<String> = Vec::new();
        if let Some(value) = x_real_ip {
            let value = value.trim();
            if !value.is_empty() {
                ips.push(value.to_string());
            }
        }
        if let Some(value) = x_forwarded_for {
            for part in value.split(',') {
                let part = part.trim();
                if !part.is_empty() {
                    ips.push(part.to_string());
                }
            }
        }
        if let Some(peer) = peer {
            ips.push(peer.to_string());
        }

        let ip = ips
            .iter()
            .filter(|s| s.parse::<std::net::IpAddr>().is_ok())
            .min_by_key(|s| {
                let addr: std::net::IpAddr = s.parse().expect("filtered above");
                // Public addresses sort first, loopback last.
                match addr {
                    a if a.is_loopback() => 2,
                    std::net::IpAddr::V4(v4) if v4.is_private() => 1,
                    _ => 0,
                }
            })
            .cloned()
            .unwrap_or_else(|| ips.first().cloned().unwrap_or_default());

        let user_agent = user_agent
            .map(|ua| {
                let cleaned: String = ua
                    .chars()
                    .map(|c| if c == '\n' || c == '\t' { ' ' } else { c })
                    .take(128)
                    .collect();
                cleaned.trim().to_string()
            })
            .unwrap_or_default();

        Self {
            ips,
            ip,
            user_agent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(value: &str) -> LogTimestamp {
        LogTimestamp(value.parse().unwrap())
    }

    #[test]
    fn formats_instant_without_fraction() {
        assert_eq!(ts("2023-10-11T12:13:14Z").to_string(), "2023-10-11T12:13:14Z");
    }

    #[test]
    fn formats_instant_with_millis() {
        assert_eq!(
            ts("2021-07-20T02:08:00.123Z").to_string(),
            "2021-07-20T02:08:00.123Z"
        );
    }

    #[test]
    fn formats_instant_with_micros() {
        assert_eq!(
            ts("2025-08-21T23:10:31.903307Z").to_string(),
            "2025-08-21T23:10:31.903307Z"
        );
    }

    #[test]
    fn trims_trailing_fraction_zeros() {
        // Shortest form, not the legacy 3-digit grouping (which would print
        // ".120") — an accepted deviation, still valid RFC-3339.
        assert_eq!(
            ts("2021-07-20T02:08:00.120Z").to_string(),
            "2021-07-20T02:08:00.12Z"
        );
    }

    #[test]
    fn now_is_truncated_to_microseconds() {
        assert_eq!(LogTimestamp::now().0.subsec_nanosecond() % 1_000, 0);
    }

    #[test]
    fn remote_prefers_public_ip() {
        let remote = Remote::from_request(
            Some("127.0.0.1".parse().unwrap()),
            Some("8.8.8.8"),
            Some("10.0.0.1, 127.0.0.1"),
            Some("test-agent"),
        );
        assert_eq!(remote.ip, "8.8.8.8");
        assert_eq!(remote.ips, vec!["8.8.8.8", "10.0.0.1", "127.0.0.1", "127.0.0.1"]);
        assert_eq!(remote.user_agent, "test-agent");
    }

    #[test]
    fn remote_falls_back_to_peer() {
        let remote = Remote::from_request(Some("127.0.0.1".parse().unwrap()), None, None, None);
        assert_eq!(remote.ip, "127.0.0.1");
        assert_eq!(remote.ips, vec!["127.0.0.1"]);
        assert_eq!(remote.user_agent, "");
    }

    #[test]
    fn user_agent_is_cleaned_and_truncated() {
        let long = "a".repeat(200);
        let remote = Remote::from_request(None, None, None, Some(&long));
        assert_eq!(remote.user_agent.len(), 128);

        let remote = Remote::from_request(None, None, None, Some("agent\nwith\tbreaks "));
        assert_eq!(remote.user_agent, "agent with breaks");
    }
}
