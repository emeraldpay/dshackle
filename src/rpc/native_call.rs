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

//! Processes individual `NativeCallItem` requests by forwarding them to an
//! upstream as JSON-RPC and converting the result back to a gRPC reply.

use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::traits::RpcUpstream;
use emerald_api::proto::blockchain::{NativeCallItem, NativeCallReplyItem};

/// Execute a single native call item against the given upstream.
///
/// Converts the gRPC call item to a JSON-RPC request, sends it to the upstream,
/// and wraps the result (or error) into a `NativeCallReplyItem`.
pub async fn execute_native_call(
    upstream: &dyn RpcUpstream,
    item: &NativeCallItem,
) -> NativeCallReplyItem {
    let params = match parse_payload(&item.payload) {
        Ok(v) => v,
        Err(msg) => {
            return NativeCallReplyItem {
                id: item.id,
                succeed: false,
                payload: Vec::new(),
                error_message: msg,
                signature: None,
            };
        }
    };

    let request = JsonRpcRequest::new(item.id, item.method.clone(), params);

    match upstream.call(&request).await {
        Ok(resp) => {
            if let Some(result) = resp.result {
                let payload = serde_json::to_vec(&result).unwrap_or_default();
                NativeCallReplyItem {
                    id: item.id,
                    succeed: true,
                    payload,
                    error_message: String::new(),
                    signature: None,
                }
            } else if let Some(err) = resp.error {
                NativeCallReplyItem {
                    id: item.id,
                    succeed: false,
                    payload: Vec::new(),
                    error_message: err.to_string(),
                    signature: None,
                }
            } else {
                // Neither result nor error — treat as null result
                NativeCallReplyItem {
                    id: item.id,
                    succeed: true,
                    payload: b"null".to_vec(),
                    error_message: String::new(),
                    signature: None,
                }
            }
        }
        Err(e) => NativeCallReplyItem {
            id: item.id,
            succeed: false,
            payload: Vec::new(),
            error_message: e.to_string(),
            signature: None,
        },
    }
}

/// Parse the raw payload bytes from the gRPC request into a `serde_json::Value`.
///
/// The payload is expected to be UTF-8 encoded JSON (typically an array of params).
/// An empty payload is treated as an empty array `[]`.
fn parse_payload(payload: &[u8]) -> Result<serde_json::Value, String> {
    if payload.is_empty() {
        return Ok(serde_json::Value::Array(Vec::new()));
    }
    let text = std::str::from_utf8(payload)
        .map_err(|e| format!("payload is not valid UTF-8: {e}"))?;
    serde_json::from_str(text)
        .map_err(|e| format!("payload is not valid JSON: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_payload() {
        let result = parse_payload(b"").unwrap();
        assert_eq!(result, serde_json::json!([]));
    }

    #[test]
    fn parse_array_payload() {
        let result = parse_payload(br#"["0xdead","latest"]"#).unwrap();
        assert_eq!(result, serde_json::json!(["0xdead", "latest"]));
    }

    #[test]
    fn parse_object_payload() {
        let result = parse_payload(br#"{"to":"0xdead","data":"0x01"}"#).unwrap();
        assert!(result.is_object());
    }

    #[test]
    fn parse_invalid_utf8() {
        let result = parse_payload(&[0xFF, 0xFE]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("UTF-8"));
    }

    #[test]
    fn parse_invalid_json() {
        let result = parse_payload(b"not json");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("JSON"));
    }
}
