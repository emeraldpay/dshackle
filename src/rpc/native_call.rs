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

//! Processes individual `NativeCallItem` requests by routing them through a
//! `Multistream` with a method-specific quorum policy, then converting the
//! result back to a gRPC reply.

use crate::jsonrpc::JsonRpcRequest;
use crate::signature::ResponseSigner;
use crate::upstream::Multistream;
use crate::upstream::router::{self, Routed};
use crate::upstream::traits::UpstreamError;
use emerald_api::proto::blockchain::{
    NativeCallItem, NativeCallReplyItem, NativeCallReplySignature,
};

/// Route a single JSON-RPC request through a chain's upstreams using the
/// method's quorum policy. The shared execution core behind both the gRPC
/// `NativeCall` and the JSON-RPC HTTP proxy.
pub async fn execute_call(
    multistream: &Multistream,
    request: &JsonRpcRequest,
) -> Result<Routed, UpstreamError> {
    let quorum = multistream.quorum_for(&request.method);
    let candidates = multistream.select_for(quorum.selector(), &request.method);
    router::route(candidates, quorum, request).await
}

/// Execute a single native call item against the upstreams of a chain.
///
/// Looks up the per-method `CallQuorum` from the `Multistream`'s factory,
/// asks the quorum which selector to use, and routes through the matching
/// candidate set. A successful result is signed when the client requested it
/// (a non-zero `nonce`) and a signer is configured.
pub async fn execute_native_call(
    multistream: &Multistream,
    item: &NativeCallItem,
    signer: Option<&ResponseSigner>,
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

    let request = JsonRpcRequest::new(item.id, item.method.clone().into(), params);
    tracing::trace!(id = item.id, method = %item.method, "executing native call item");

    match execute_call(multistream, &request).await {
        Ok(routed) => {
            let resp = routed.response;
            if let Some(result) = resp.result {
                // Forward the raw JSON bytes as-is, without re-serialization
                let payload = result.get().as_bytes().to_vec();
                tracing::trace!(id = item.id, method = %item.method, bytes = payload.len(), "call succeeded");
                let signature = build_signature(signer, item.nonce, &payload, routed.source);
                NativeCallReplyItem {
                    id: item.id,
                    succeed: true,
                    payload,
                    error_message: String::new(),
                    signature,
                }
            } else if let Some(err) = resp.error {
                tracing::trace!(id = item.id, method = %item.method, error = %err, "call returned rpc error");
                // Forward the JSON-RPC error object verbatim so callers see the
                // upstream's exact `code`/`message`/`data`
                let error_message = serde_json::to_string(&err).unwrap_or_else(|_| err.to_string());
                NativeCallReplyItem {
                    id: item.id,
                    succeed: false,
                    payload: Vec::new(),
                    error_message,
                    signature: None,
                }
            } else {
                // Neither result nor error — treat as null result
                tracing::trace!(id = item.id, method = %item.method, "call returned null result");
                NativeCallReplyItem {
                    id: item.id,
                    succeed: true,
                    payload: b"null".to_vec(),
                    error_message: String::new(),
                    signature: None,
                }
            }
        }
        Err(e) => {
            tracing::trace!(id = item.id, method = %item.method, error = %e, "call failed");
            NativeCallReplyItem {
                id: item.id,
                succeed: false,
                payload: Vec::new(),
                error_message: e.to_string(),
                signature: None,
            }
        }
    }
}

/// Signs a successful result when the client asked for it. A `nonce` of 0
/// means "no signature requested" (proto3 default), matching the legacy
/// behavior; without a configured signer the reply simply carries none.
fn build_signature(
    signer: Option<&ResponseSigner>,
    nonce: u64,
    payload: &[u8],
    source: Option<String>,
) -> Option<NativeCallReplySignature> {
    if nonce == 0 {
        return None;
    }
    let signer = signer?;
    let Some(source) = source else {
        // Should not happen: every quorum records the source of the response
        // it resolves with. Better an unsigned reply than a wrong attribution.
        tracing::warn!("Response source unknown, skipping signature");
        return None;
    };
    let signature = signer.sign(nonce, payload, &source)?;
    Some(NativeCallReplySignature {
        nonce,
        signature: signature.value,
        key_id: signature.key_id,
        upstream_id: signature.upstream_id,
    })
}

/// Parse the raw payload bytes from the gRPC request into a `serde_json::Value`.
///
/// The payload is expected to be UTF-8 encoded JSON (typically an array of params).
/// An empty payload is treated as an empty array `[]`.
fn parse_payload(payload: &[u8]) -> Result<serde_json::Value, String> {
    if payload.is_empty() {
        return Ok(serde_json::Value::Array(Vec::new()));
    }
    let text =
        std::str::from_utf8(payload).map_err(|e| format!("payload is not valid UTF-8: {e}"))?;
    serde_json::from_str(text).map_err(|e| format!("payload is not valid JSON: {e}"))
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
