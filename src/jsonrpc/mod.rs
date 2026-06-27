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

//! JSON-RPC 2.0 wire types for communicating with upstream blockchain nodes.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fmt;
use std::str::FromStr;

// ─── RPC method name ───────────────────────────────────────────────────────

/// A validated JSON-RPC method name (e.g. `eth_getBalance`, `net_version`).
///
/// Wraps a `String` with the guarantee that it is non-empty and contains only
/// ASCII alphanumeric characters and underscores.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RpcMethod(String);

impl RpcMethod {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RpcMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for RpcMethod {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err("RPC method name must not be empty".into());
        }
        if !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(format!(
                "RPC method name must contain only ASCII alphanumeric characters and underscores, got: {s}"
            ));
        }
        Ok(RpcMethod(s.to_string()))
    }
}

impl From<&str> for RpcMethod {
    /// Creates an `RpcMethod` from a string slice.
    ///
    /// # Panics
    ///
    /// Panics if the string is not a valid RPC method name.
    /// Use [`FromStr`] for fallible conversion of runtime strings.
    fn from(s: &str) -> Self {
        s.parse().expect("invalid RPC method name")
    }
}

impl From<String> for RpcMethod {
    /// Creates an `RpcMethod` from a `String`.
    ///
    /// # Panics
    ///
    /// Panics if the string is not a valid RPC method name.
    fn from(s: String) -> Self {
        Self::from(s.as_str())
    }
}

impl From<RpcMethod> for String {
    fn from(m: RpcMethod) -> Self {
        m.0
    }
}

impl AsRef<str> for RpcMethod {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for RpcMethod {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RpcMethod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

// ─── JSON-RPC request / response ──────────────────────────────────���────────

/// Outgoing JSON-RPC 2.0 request sent to an upstream node.
#[derive(Debug, Serialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: &'static str,
    pub id: u32,
    pub method: RpcMethod,
    pub params: serde_json::Value,
}

impl JsonRpcRequest {
    pub fn new(id: u32, method: RpcMethod, params: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            method,
            params,
        }
    }
}

/// Incoming JSON-RPC 2.0 response from an upstream node.
///
/// The `result` field is kept as raw JSON bytes (`RawValue`) to avoid
/// deserializing and re-serializing upstream data — preserving it exactly
/// as the upstream returned it.
#[derive(Debug, Deserialize)]
pub struct JsonRpcResponse {
    #[allow(dead_code)]
    pub id: serde_json::Value,
    pub result: Option<Box<RawValue>>,
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    /// Extract the result as a JSON string value (e.g. `"0x1f"` → `"0x1f"`).
    /// Returns `None` if the response carries an error, has no result, or
    /// the result is not a JSON string.
    pub fn result_as_string(&self) -> Option<String> {
        if self.error.is_some() {
            return None;
        }
        let raw = self.result.as_ref()?;
        serde_json::from_str::<String>(raw.get()).ok()
    }

    /// Whether the response carries a non-null, non-error result. A JSON
    /// `null` result deserializes to `None`, so any `Some(_)` payload counts
    /// as non-empty here.
    pub fn is_non_empty_result(&self) -> bool {
        self.error.is_none() && self.result.is_some()
    }
}

/// JSON-RPC 2.0 error object.
///
/// The `data` field is omitted on serialize when `None` to avoid emitting
/// `"data":null` for upstreams that didn't include it in the original error.
#[derive(Debug, Deserialize, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl std::fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JSON-RPC error {}: {}", self.code, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── RpcMethod ─────────────────────────────────────────────────────

    #[test]
    fn rpc_method_accepts_valid_names() {
        assert!("eth_getBalance".parse::<RpcMethod>().is_ok());
        assert!("net_version".parse::<RpcMethod>().is_ok());
        assert!("web3_clientVersion".parse::<RpcMethod>().is_ok());
        assert!("eth_blockNumber".parse::<RpcMethod>().is_ok());
        assert!("eth_sendRawTransaction".parse::<RpcMethod>().is_ok());
        assert!("getblock".parse::<RpcMethod>().is_ok());
    }

    #[test]
    fn rpc_method_rejects_empty() {
        assert!("".parse::<RpcMethod>().is_err());
    }

    #[test]
    fn rpc_method_rejects_spaces() {
        assert!("eth getBalance".parse::<RpcMethod>().is_err());
    }

    #[test]
    fn rpc_method_rejects_special_chars() {
        assert!("eth.getBalance".parse::<RpcMethod>().is_err());
        assert!("eth-getBalance".parse::<RpcMethod>().is_err());
        assert!("eth/getBalance".parse::<RpcMethod>().is_err());
    }

    #[test]
    fn rpc_method_display_and_as_str() {
        let m: RpcMethod = "eth_getBalance".into();
        assert_eq!(m.as_str(), "eth_getBalance");
        assert_eq!(m.to_string(), "eth_getBalance");
    }

    #[test]
    fn rpc_method_serialize_as_string() {
        let m: RpcMethod = "eth_getBalance".into();
        let json = serde_json::to_value(&m).unwrap();
        assert_eq!(json, "eth_getBalance");
    }

    #[test]
    fn rpc_method_deserialize_valid() {
        let m: RpcMethod = serde_json::from_str("\"eth_getBalance\"").unwrap();
        assert_eq!(m.as_str(), "eth_getBalance");
    }

    #[test]
    fn rpc_method_deserialize_rejects_invalid() {
        let result = serde_json::from_str::<RpcMethod>("\"eth getBalance\"");
        assert!(result.is_err());
    }

    // ── JsonRpcRequest ──────────────────────────────────────────────

    #[test]
    fn serialize_request_with_params() {
        let req = JsonRpcRequest::new(
            1,
            "eth_getBalance".into(),
            serde_json::json!(["0xdead", "latest"]),
        );
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["jsonrpc"], "2.0");
        assert_eq!(json["id"], 1);
        assert_eq!(json["method"], "eth_getBalance");
        assert_eq!(json["params"][0], "0xdead");
        assert_eq!(json["params"][1], "latest");
    }

    #[test]
    fn serialize_request_with_empty_params() {
        let req = JsonRpcRequest::new(42, "eth_blockNumber".into(), serde_json::json!([]));
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["id"], 42);
        assert_eq!(json["method"], "eth_blockNumber");
        assert!(json["params"].as_array().unwrap().is_empty());
    }

    #[test]
    fn deserialize_success_response() {
        let raw = r#"{"jsonrpc":"2.0","id":1,"result":"0x72fa5e0181"}"#;
        let resp: JsonRpcResponse = serde_json::from_str(raw).unwrap();
        assert!(resp.error.is_none());
        // RawValue preserves the exact JSON text including quotes
        assert_eq!(resp.result.unwrap().get(), r#""0x72fa5e0181""#);
    }

    #[test]
    fn deserialize_error_response() {
        let raw =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(raw).unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32601);
        assert_eq!(err.message, "Method not found");
        assert!(err.data.is_none());
    }

    #[test]
    fn deserialize_error_response_with_data() {
        let raw = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted","data":"0xdead"}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(raw).unwrap();
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32000);
        assert_eq!(err.data.unwrap(), "0xdead");
    }

    #[test]
    fn display_jsonrpc_error() {
        let err = JsonRpcError {
            code: -32601,
            message: "Method not found".to_string(),
            data: None,
        };
        assert_eq!(err.to_string(), "JSON-RPC error -32601: Method not found");
    }

    #[test]
    fn serialize_jsonrpc_error_with_data() {
        let err = JsonRpcError {
            code: 3,
            message: "execution reverted".to_string(),
            data: Some(serde_json::json!("0x")),
        };
        let json = serde_json::to_string(&err).unwrap();
        assert_eq!(
            json,
            r#"{"code":3,"message":"execution reverted","data":"0x"}"#
        );
    }

    #[test]
    fn serialize_jsonrpc_error_omits_absent_data() {
        // Upstreams that don't include `data` should not see `"data":null`
        // appear in the forwarded error_message — preserve the original shape.
        let err = JsonRpcError {
            code: -32601,
            message: "Method not found".to_string(),
            data: None,
        };
        let json = serde_json::to_string(&err).unwrap();
        assert_eq!(json, r#"{"code":-32601,"message":"Method not found"}"#);
    }

    // ── Response parsing tests (ported from legacy ResponseRpcParserTest) ──

    #[test]
    fn parse_string_result() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": "Hello world!"}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap().get(), r#""Hello world!""#);
    }

    #[test]
    fn parse_string_result_when_result_first() {
        let json = r#"{"result": "Hello world!", "jsonrpc": "2.0", "id": 1}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap().get(), r#""Hello world!""#);
    }

    #[test]
    fn parse_bool_result() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": false}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap().get(), "false");
    }

    #[test]
    fn parse_bool_result_when_id_last() {
        let json = r#"{"jsonrpc": "2.0", "result": false, "id": 1}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap().get(), "false");
    }

    #[test]
    fn parse_int_result() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": 100}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap().get(), "100");
    }

    #[test]
    fn parse_null_result() {
        // With Option<Box<RawValue>>, "result": null deserializes to None.
        // Our native_call layer handles this by returning b"null" payload.
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": null}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert!(resp.result.is_none());
    }

    #[test]
    fn parse_object_result() {
        let json =
            r#"{"jsonrpc": "2.0", "id": 1, "result": {"hash": "0x00000", "foo": false, "bar": 1}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(
            resp.result.unwrap().get(),
            r#"{"hash": "0x00000", "foo": false, "bar": 1}"#
        );
    }

    #[test]
    fn parse_object_result_with_null_error() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": {"hash": "0x00000", "foo": false, "bar": 1}, "error": null}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(
            resp.result.unwrap().get(),
            r#"{"hash": "0x00000", "foo": false, "bar": 1}"#
        );
    }

    #[test]
    fn parse_object_result_when_null_error_first() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "error": null, "result": {"hash": "0x00000", "foo": false, "bar": 1}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(
            resp.result.unwrap().get(),
            r#"{"hash": "0x00000", "foo": false, "bar": 1}"#
        );
    }

    #[test]
    fn parse_object_result_preserves_whitespace() {
        let json = r#"{"jsonrpc": "2.0", "result"  :   {"hash": "0x00000", "foo": false , "bar":1}  , "id": 1}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        // RawValue preserves the exact whitespace from the original JSON
        assert_eq!(
            resp.result.unwrap().get(),
            r#"{"hash": "0x00000", "foo": false , "bar":1}"#
        );
    }

    #[test]
    fn parse_complex_nested_object_result() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": {"hash": "0x00000", "foo": {"bar": 1, "baz": 2}}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(
            resp.result.unwrap().get(),
            r#"{"hash": "0x00000", "foo": {"bar": 1, "baz": 2}}"#
        );
    }

    #[test]
    fn parse_array_result() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": [1, 2, false]}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap().get(), "[1, 2, false]");
    }

    #[test]
    fn parse_error_with_null_result() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": null, "error": {"code": -1111, "message": "test"}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -1111);
        assert_eq!(err.message, "test");
    }

    #[test]
    fn parse_error_without_result_field() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "error": {"code": -1111, "message": "test"}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -1111);
        assert_eq!(err.message, "test");
    }

    #[test]
    fn parse_error_with_string_data() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": null, "error": {"code": -1111, "message": "test", "data": "just data"}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        let err = resp.error.unwrap();
        assert_eq!(err.code, -1111);
        assert_eq!(err.message, "test");
        assert_eq!(err.data.unwrap(), "just data");
    }

    #[test]
    fn parse_error_with_object_data() {
        let json = r#"{"jsonrpc": "2.0", "id": 1, "result": null, "error": {"code": -1111, "message": "test", "data": {"foo": "just data", "bar": 1}}}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        let err = resp.error.unwrap();
        assert_eq!(err.code, -1111);
        let data = err.data.unwrap();
        assert_eq!(data["foo"], "just data");
        assert_eq!(data["bar"], 1);
    }

    #[test]
    fn reject_non_json() {
        let result = serde_json::from_str::<JsonRpcResponse>("NOT JSON");
        assert!(result.is_err());
    }

    #[test]
    fn reject_incomplete_json() {
        let result = serde_json::from_str::<JsonRpcResponse>(r#"{"jsonrpc": "2.0", "id": 1}"#);
        // serde accepts this — result and error are both None (all fields optional)
        let resp = result.unwrap();
        assert!(resp.result.is_none());
        assert!(resp.error.is_none());
    }

    #[test]
    fn reject_broken_json() {
        let result =
            serde_json::from_str::<JsonRpcResponse>(r#"{"jsonrpc": "2.0", "id": 101, "resu'"#);
        assert!(result.is_err());
    }

    #[test]
    fn parse_json_with_leading_spaces() {
        let json = r#"  {"result": "Hello world!", "jsonrpc": "2.0", "id": 1}"#;
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap().get(), r#""Hello world!""#);
    }

    #[test]
    fn parse_json_with_leading_newline() {
        let json = "\n{\"result\": \"Hello world!\", \"jsonrpc\": \"2.0\", \"id\": 1}";
        let resp: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap().get(), r#""Hello world!""#);
    }

    // ── result_as_string / is_non_empty_result ─────────────────────────

    fn parse_resp(json: &str) -> JsonRpcResponse {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn result_as_string_extracts_string_value() {
        let r = parse_resp(r#"{"jsonrpc":"2.0","id":1,"result":"0xdead"}"#);
        assert_eq!(r.result_as_string().as_deref(), Some("0xdead"));
    }

    #[test]
    fn result_as_string_none_for_non_string() {
        let r = parse_resp(r#"{"jsonrpc":"2.0","id":1,"result":123}"#);
        assert_eq!(r.result_as_string(), None);
    }

    #[test]
    fn result_as_string_none_for_error_response() {
        let r = parse_resp(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"x"}}"#);
        assert_eq!(r.result_as_string(), None);
    }

    #[test]
    fn result_as_string_none_for_null_result() {
        let r = parse_resp(r#"{"jsonrpc":"2.0","id":1,"result":null}"#);
        assert_eq!(r.result_as_string(), None);
    }

    #[test]
    fn is_non_empty_true_for_string() {
        let r = parse_resp(r#"{"jsonrpc":"2.0","id":1,"result":"hi"}"#);
        assert!(r.is_non_empty_result());
    }

    #[test]
    fn is_non_empty_true_for_object() {
        let r = parse_resp(r#"{"jsonrpc":"2.0","id":1,"result":{"a":1}}"#);
        assert!(r.is_non_empty_result());
    }

    #[test]
    fn is_non_empty_false_for_null() {
        let r = parse_resp(r#"{"jsonrpc":"2.0","id":1,"result":null}"#);
        assert!(!r.is_non_empty_result());
    }

    #[test]
    fn is_non_empty_false_for_error() {
        let r = parse_resp(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"x"}}"#);
        assert!(!r.is_non_empty_result());
    }
}
