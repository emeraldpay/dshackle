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

//! Request log (ingress log) record: one JSON-RPC request Dshackle made to an
//! upstream. Replicates the legacy `RequestRecord.BlockchainRequest` schema.

use crate::logs::record::{Channel, LogTimestamp};
use serde::Serialize;
use uuid::Uuid;

/// The version stamp of every request log record.
pub const VERSION: &str = "requestlog/v1alpha";

/// One request to an upstream, written on its completion (success or error).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestRecord {
    pub version: &'static str,
    pub id: Uuid,
    pub success: bool,
    pub upstream: UpstreamDetails,
    pub request: IngressRequestDetails,
    pub jsonrpc: JsonRpcDetails,
    pub blockchain: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execute: Option<LogTimestamp>,
    pub complete: LogTimestamp,
    pub response_size: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetails>,
    pub queue_time: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_time: Option<f64>,
}

/// The upstream connection that carried the request. `request_type` is always
/// `JSONRPC`, as the legacy ingress processor stamped it.
#[derive(Clone, Debug, Serialize)]
pub struct UpstreamDetails {
    pub id: String,
    pub channel: Channel,
    #[serde(rename = "type")]
    pub request_type: &'static str,
}

/// The client request this upstream request serves, or a Dshackle-internal
/// origin (head polls, validation probes).
#[derive(Clone, Debug, Serialize)]
pub struct IngressRequestDetails {
    pub source: Source,
    pub id: Uuid,
    pub start: LogTimestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Source {
    /// Serving a client request.
    #[serde(rename = "REQUEST")]
    Request,
    /// Dshackle's own background activity.
    #[serde(rename = "INTERNAL")]
    Internal,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcDetails {
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<String>,
    pub id: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ErrorDetails {
    pub code: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Milliseconds between two timestamps computed the way the legacy record did:
/// from the nanosecond-of-second fields only, clamped at zero. The values wrap
/// across second boundaries; kept bug-compatible so the log consumers see the
/// same distribution.
pub fn legacy_elapsed_ms(from: &LogTimestamp, to: &LogTimestamp) -> f64 {
    let nanos = (to.0.subsec_nanosecond() as i64 - from.0.subsec_nanosecond() as i64).max(0);
    nanos as f64 / 1_000_000.0
}

/// The `params` value when `include-params` is on: JSON, abbreviated in the
/// middle to 400 characters like the legacy `StringUtils.abbreviateMiddle`.
pub fn abbreviated_params(params: &serde_json::Value) -> String {
    let json = params.to_string();
    if json.chars().count() <= 400 {
        return json;
    }
    let chars: Vec<char> = json.chars().collect();
    // Legacy keeps ceil((max - 2) / 2) leading chars and the rest trailing.
    let target = 400 - 2;
    let head = target / 2 + target % 2;
    let tail = target - head;
    let mut out: String = chars[..head].iter().collect();
    out.push_str("..");
    out.extend(&chars[chars.len() - tail..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(value: &str) -> LogTimestamp {
        LogTimestamp(value.parse().unwrap())
    }

    /// Byte-for-byte comparison against a real record written by the legacy
    /// implementation (from the acceptance-testing sample logs).
    #[test]
    fn matches_legacy_sample() {
        let start = ts("2025-08-21T23:10:06.131276Z");
        let execute = ts("2025-08-21T23:10:05.725574Z");
        let complete = ts("2025-08-21T23:10:06.132159Z");
        let record = RequestRecord {
            version: VERSION,
            id: "a0004914-b55a-4ff9-9890-373306cb6515".parse().unwrap(),
            success: true,
            upstream: UpstreamDetails {
                id: "eth-1".to_string(),
                channel: Channel::JsonRpc,
                request_type: "JSONRPC",
            },
            request: IngressRequestDetails {
                source: Source::Internal,
                id: "e5690cf1-e056-40de-a4e1-1bcaeedf74fb".parse().unwrap(),
                start,
            },
            jsonrpc: JsonRpcDetails {
                method: "eth_blockNumber".to_string(),
                params: None,
                id: 0,
            },
            blockchain: "ETHEREUM",
            execute: Some(execute),
            complete,
            response_size: 11,
            error: None,
            queue_time: legacy_elapsed_ms(&start, &execute),
            request_time: Some(legacy_elapsed_ms(&execute, &complete)),
        };

        let expected = r#"{"version":"requestlog/v1alpha","id":"a0004914-b55a-4ff9-9890-373306cb6515","success":true,"upstream":{"id":"eth-1","channel":"JSONRPC","type":"JSONRPC"},"request":{"source":"INTERNAL","id":"e5690cf1-e056-40de-a4e1-1bcaeedf74fb","start":"2025-08-21T23:10:06.131276Z"},"jsonrpc":{"method":"eth_blockNumber","id":0},"blockchain":"ETHEREUM","execute":"2025-08-21T23:10:05.725574Z","complete":"2025-08-21T23:10:06.132159Z","responseSize":11,"queueTime":594.298,"requestTime":0.0}"#;
        assert_eq!(serde_json::to_string(&record).unwrap(), expected);
    }

    #[test]
    fn elapsed_uses_only_nanos_of_second() {
        let a = ts("2025-08-21T23:10:06.131276Z");
        let b = ts("2025-08-21T23:10:05.725574Z");
        // Only the fractional parts are compared (.131276 vs .725574): the
        // wall-clock ordering of the full timestamps is ignored, and a
        // negative difference clamps to zero — the legacy quirk this replicates.
        assert_eq!(legacy_elapsed_ms(&a, &b), 594.298);
        assert_eq!(legacy_elapsed_ms(&b, &a), 0.0);
    }

    #[test]
    fn abbreviates_long_params_in_the_middle() {
        let long = serde_json::Value::String("x".repeat(500));
        let abbreviated = abbreviated_params(&long);
        assert_eq!(abbreviated.chars().count(), 400);
        assert!(abbreviated.contains(".."));

        let short = serde_json::json!([1, 2, 3]);
        assert_eq!(abbreviated_params(&short), "[1,2,3]");
    }
}
