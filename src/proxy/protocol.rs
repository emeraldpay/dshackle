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

//! JSON-RPC 2.0 wire framing for the HTTP proxy: request sniffing, per-item
//! validation, and response serialization. Ports the legacy `ReadRpcJson` /
//! `WriteRpcJson` behavior, including its exact error codes and messages so the
//! proxy stays wire-compatible with the JVM implementation.

use serde::Serialize;
use serde_json::value::RawValue;

/// Malformed JSON or wrong root type.
pub const CODE_INVALID_JSON: i64 = -32700;
/// A structurally invalid request (missing id/method, bad version, …).
pub const CODE_INVALID_REQUEST: i64 = -32600;
/// The call did not complete (routing / quorum failure).
pub const CODE_CALL_FAILURE: i64 = -32003;

/// Whether the request body is a single object or a batch array.
#[derive(Debug)]
pub enum BodyKind {
    Single,
    Batch,
}

/// A JSON-RPC request id echoed back verbatim. Per the spec (and the legacy
/// proxy) only numbers and strings are accepted; `null` is rejected.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub enum RequestId {
    Num(i64),
    Str(String),
}

impl RequestId {
    /// Id used when none is available — e.g. a parse error before any id could
    /// be read. Matches the legacy `NumberId(-1)`.
    pub fn fallback() -> Self {
        RequestId::Num(-1)
    }

    fn from_value(v: &serde_json::Value) -> Option<Self> {
        match v {
            // Numbers are normalized to a 64-bit integer, as the legacy proxy does.
            serde_json::Value::Number(n) => n.as_i64().map(RequestId::Num),
            serde_json::Value::String(s) => Some(RequestId::Str(s.clone())),
            _ => None,
        }
    }
}

/// A request that failed framing or validation, carrying the code, message, and
/// the id to echo (the fallback id when none could be determined).
#[derive(Debug)]
pub struct ProtocolError {
    pub code: i64,
    pub message: String,
    pub id: RequestId,
}

impl ProtocolError {
    fn invalid_json(message: &str) -> Self {
        Self {
            code: CODE_INVALID_JSON,
            message: message.to_string(),
            id: RequestId::fallback(),
        }
    }

    fn invalid_request(message: String, id: RequestId) -> Self {
        Self {
            code: CODE_INVALID_REQUEST,
            message,
            id,
        }
    }
}

/// A validated single request ready to execute.
#[derive(Debug)]
pub struct ProxyRequest {
    pub id: RequestId,
    pub method: String,
    pub params: serde_json::Value,
}

/// Detect single vs batch from the first non-whitespace byte, scanning at most
/// the first 256 bytes. Matches the legacy sniffing exactly — note that `\r` is
/// *not* treated as whitespace.
pub fn detect_kind(body: &[u8]) -> Result<BodyKind, ProtocolError> {
    for &b in body.iter().take(256) {
        match b {
            b' ' | b'\n' | b'\t' => continue,
            b'{' => return Ok(BodyKind::Single),
            b'[' => return Ok(BodyKind::Batch),
            _ => return Err(ProtocolError::invalid_json("Failed to parse JSON")),
        }
    }
    Err(ProtocolError::invalid_json("Empty JSON"))
}

/// Parse the whole body into a JSON value, reporting a parse error with the
/// legacy code on failure.
pub fn parse_body(body: &[u8]) -> Result<serde_json::Value, ProtocolError> {
    serde_json::from_slice(body).map_err(|e| ProtocolError::invalid_json(&e.to_string()))
}

/// Validate a single request object, extracting the fields needed to execute
/// it. The checks and their messages mirror the legacy `jsonExtractor`.
pub fn validate_item(item: &serde_json::Value) -> Result<ProxyRequest, ProtocolError> {
    // id first: must be present and a number or string (null is rejected). The
    // legacy "ID is not set" error carries no id, so it echoes the fallback.
    let id = match item.get("id").and_then(RequestId::from_value) {
        Some(id) => id,
        None => {
            return Err(ProtocolError::invalid_request(
                "ID is not set".to_string(),
                RequestId::fallback(),
            ));
        }
    };

    match item.get("jsonrpc") {
        Some(serde_json::Value::String(v)) if v == "2.0" => {}
        None => {
            return Err(ProtocolError::invalid_request(
                "jsonrpc version is not set".to_string(),
                id,
            ));
        }
        Some(other) => {
            return Err(ProtocolError::invalid_request(
                format!("Unsupported JSON RPC version: {other}"),
                id,
            ));
        }
    }

    let method = match item.get("method").and_then(|v| v.as_str()) {
        Some(m) => m.to_string(),
        None => {
            return Err(ProtocolError::invalid_request(
                "Method is not set".to_string(),
                id,
            ));
        }
    };

    // Params must be an array; absent or null becomes an empty array. Object
    // params are explicitly rejected, as in the legacy proxy.
    let params = match item.get("params") {
        None | Some(serde_json::Value::Null) => serde_json::Value::Array(Vec::new()),
        Some(v @ serde_json::Value::Array(_)) => v.clone(),
        Some(_) => {
            return Err(ProtocolError::invalid_request(
                "Params must be an array".to_string(),
                id,
            ));
        }
    };

    Ok(ProxyRequest { id, method, params })
}

// ─── Response serialization ─────────────────────────────────────────────────

#[derive(Serialize)]
struct SuccessResponse<'a> {
    jsonrpc: &'static str,
    id: &'a RequestId,
    result: &'a RawValue,
}

#[derive(Serialize)]
struct ErrorResponse<'a> {
    jsonrpc: &'static str,
    id: &'a RequestId,
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

/// Serialize a success response, forwarding the upstream `result` verbatim.
pub fn build_success(id: &RequestId, result: &RawValue) -> String {
    serde_json::to_string(&SuccessResponse {
        jsonrpc: "2.0",
        id,
        result,
    })
    .expect("response serialization never fails")
}

/// Serialize a success response whose result is an owned JSON value (used for
/// proxy-generated results like a subscription id or an unsubscribe boolean,
/// where there is no upstream `RawValue` to forward).
pub fn build_success_value(id: &RequestId, value: serde_json::Value) -> String {
    let raw = RawValue::from_string(value.to_string()).expect("value serializes to valid json");
    build_success(id, &raw)
}

/// Serialize an error response.
pub fn build_error(
    id: &RequestId,
    code: i64,
    message: String,
    data: Option<serde_json::Value>,
) -> String {
    serde_json::to_string(&ErrorResponse {
        jsonrpc: "2.0",
        id,
        error: ErrorBody {
            code,
            message,
            data,
        },
    })
    .expect("response serialization never fails")
}

/// Serialize a framing/validation error into its response body.
pub fn build_protocol_error(err: &ProtocolError) -> String {
    build_error(&err.id, err.code, err.message.clone(), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_single_and_batch_ignoring_leading_whitespace() {
        assert!(matches!(detect_kind(b"  \n\t{}"), Ok(BodyKind::Single)));
        assert!(matches!(detect_kind(b"\n  []"), Ok(BodyKind::Batch)));
    }

    #[test]
    fn carriage_return_is_not_whitespace() {
        // Matches the legacy quirk: \r before the JSON is a parse error.
        let err = detect_kind(b"\r{}").unwrap_err();
        assert_eq!(err.code, CODE_INVALID_JSON);
    }

    #[test]
    fn empty_body_is_invalid_json() {
        let err = detect_kind(b"   ").unwrap_err();
        assert_eq!(err.code, CODE_INVALID_JSON);
        assert_eq!(err.message, "Empty JSON");
    }

    #[test]
    fn non_json_first_char_is_parse_error() {
        let err = detect_kind(b"garbage").unwrap_err();
        assert_eq!(err.message, "Failed to parse JSON");
    }

    #[test]
    fn valid_item_extracts_fields() {
        let item =
            serde_json::json!({"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]});
        let req = validate_item(&item).unwrap();
        assert_eq!(req.id, RequestId::Num(7));
        assert_eq!(req.method, "eth_blockNumber");
    }

    #[test]
    fn string_id_is_preserved() {
        let item = serde_json::json!({"jsonrpc":"2.0","id":"abc","method":"m"});
        let req = validate_item(&item).unwrap();
        assert_eq!(req.id, RequestId::Str("abc".to_string()));
    }

    #[test]
    fn missing_id_is_invalid_request() {
        let item = serde_json::json!({"jsonrpc":"2.0","method":"m"});
        let err = validate_item(&item).unwrap_err();
        assert_eq!(err.code, CODE_INVALID_REQUEST);
        assert_eq!(err.message, "ID is not set");
    }

    #[test]
    fn null_id_is_rejected() {
        let item = serde_json::json!({"jsonrpc":"2.0","id":null,"method":"m"});
        let err = validate_item(&item).unwrap_err();
        assert_eq!(err.message, "ID is not set");
    }

    #[test]
    fn wrong_version_is_rejected_with_id_echoed() {
        let item = serde_json::json!({"jsonrpc":"1.0","id":3,"method":"m"});
        let err = validate_item(&item).unwrap_err();
        assert_eq!(err.code, CODE_INVALID_REQUEST);
        assert_eq!(err.id, RequestId::Num(3));
        assert!(err.message.contains("Unsupported JSON RPC version"));
    }

    #[test]
    fn missing_version_is_rejected() {
        let item = serde_json::json!({"id":3,"method":"m"});
        let err = validate_item(&item).unwrap_err();
        assert_eq!(err.message, "jsonrpc version is not set");
    }

    #[test]
    fn missing_method_is_rejected() {
        let item = serde_json::json!({"jsonrpc":"2.0","id":3});
        let err = validate_item(&item).unwrap_err();
        assert_eq!(err.message, "Method is not set");
    }

    #[test]
    fn object_params_are_rejected() {
        let item = serde_json::json!({"jsonrpc":"2.0","id":3,"method":"m","params":{"a":1}});
        let err = validate_item(&item).unwrap_err();
        assert_eq!(err.message, "Params must be an array");
    }

    #[test]
    fn absent_params_default_to_empty_array() {
        let item = serde_json::json!({"jsonrpc":"2.0","id":3,"method":"m"});
        let req = validate_item(&item).unwrap();
        assert_eq!(req.params, serde_json::json!([]));
    }

    #[test]
    fn success_response_forwards_raw_result() {
        let raw = RawValue::from_string(r#"{"a":1}"#.to_string()).unwrap();
        let body = build_success(&RequestId::Num(1), &raw);
        assert_eq!(body, r#"{"jsonrpc":"2.0","id":1,"result":{"a":1}}"#);
    }

    #[test]
    fn error_response_omits_absent_data() {
        let body = build_error(
            &RequestId::Str("x".into()),
            -32601,
            "Method not found".into(),
            None,
        );
        assert_eq!(
            body,
            r#"{"jsonrpc":"2.0","id":"x","error":{"code":-32601,"message":"Method not found"}}"#
        );
    }
}
