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

/// Outgoing JSON-RPC 2.0 request sent to an upstream node.
#[derive(Debug, Serialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: &'static str,
    pub id: u32,
    pub method: String,
    pub params: serde_json::Value,
}

impl JsonRpcRequest {
    pub fn new(id: u32, method: String, params: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            method,
            params,
        }
    }
}

/// Incoming JSON-RPC 2.0 response from an upstream node.
#[derive(Debug, Deserialize)]
pub struct JsonRpcResponse {
    #[allow(dead_code)]
    pub id: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub error: Option<JsonRpcError>,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
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

    #[test]
    fn serialize_request_with_params() {
        let req = JsonRpcRequest::new(
            1,
            "eth_getBalance".to_string(),
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
        let req = JsonRpcRequest::new(42, "eth_blockNumber".to_string(), serde_json::json!([]));
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
        assert_eq!(resp.result.unwrap(), "0x72fa5e0181");
    }

    #[test]
    fn deserialize_error_response() {
        let raw = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#;
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
}
