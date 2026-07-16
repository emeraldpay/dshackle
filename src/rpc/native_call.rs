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
use crate::signature::{ProvidedSignature, ResponseSigner};
use crate::upstream::egress::ChainAccess;
use crate::upstream::ethereum::call_selector;
use crate::upstream::router::{self, Routed};
use crate::upstream::selector::LabelSelector;
use crate::upstream::traits::UpstreamError;
use crate::upstream::{Multistream, UpstreamId, multistream};
use emerald_api::proto::blockchain::{
    NativeCallItem, NativeCallReplyItem, NativeCallReplySignature,
};

/// Route a single JSON-RPC request through a chain's upstreams using the
/// method's quorum policy. The shared execution core behind both the gRPC
/// `NativeCall` and the JSON-RPC HTTP proxy (which always passes
/// [`LabelSelector::Any`] — label selection is a gRPC-only feature, as in
/// legacy).
///
/// Candidates must pass the client's label selector, and — for Ethereum state
/// reads pinned to a block — must have reached that block. Both are hard
/// filters: sending an `eth_call` for a historic block to a pruned node would
/// return a wrong (empty) result rather than an error, so an empty candidate
/// list must fail the request instead of falling back.
pub async fn execute_call(
    multistream: &Multistream,
    request: &JsonRpcRequest,
    labels: &LabelSelector,
) -> Result<Routed, UpstreamError> {
    let quorum = multistream.quorum_for(&request.method);
    let mut candidates = multistream.select_for(quorum.selector(), &request.method);
    if *labels != LabelSelector::Any {
        candidates.retain(|u| labels.matches_any_set(u.label_sets()));
    }
    let min_height =
        call_selector::min_height_for(request, || ChainAccess::current_height(multistream));
    if let Some(min_height) = min_height {
        candidates = multistream::at_height(candidates, min_height);
    }
    router::route(multistream.chain(), candidates, quorum, request).await
}

/// Execute a single native call item against the upstreams of a chain.
///
/// Looks up the per-method `CallQuorum` from the `Multistream`'s factory,
/// asks the quorum which selector to use, and routes through the matching
/// candidate set filtered by the request's label selector. A successful
/// result is signed when the client requested it (a non-zero `nonce`) and a
/// signer is configured.
pub async fn execute_native_call(
    multistream: &Multistream,
    item: &NativeCallItem,
    labels: &LabelSelector,
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

    let mut request = JsonRpcRequest::new(item.id, item.method.clone().into(), params);
    // Carry the client's nonce down to the upstream so a remote Dshackle can
    // sign its reply over it; other upstream types ignore it.
    request.nonce = item.nonce;
    tracing::trace!(id = item.id, method = %item.method, "executing native call item");

    match execute_call(multistream, &request, labels).await {
        Ok(routed) => {
            let resp = routed.response;
            let provided = resp.provided_signature;
            if let Some(err) = resp.error {
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
                // A present result forwards its raw JSON bytes as-is; a missing
                // one becomes `null`. Both are signed (legacy signs over
                // `resultOrEmpty`), so a null result the client asked to be
                // signed still carries a signature.
                let payload = match resp.result {
                    Some(result) => result.get().as_bytes().to_vec(),
                    None => b"null".to_vec(),
                };
                tracing::trace!(id = item.id, method = %item.method, bytes = payload.len(), "call succeeded");
                let signature =
                    build_signature(signer, item.nonce, &payload, routed.source, provided);
                NativeCallReplyItem {
                    id: item.id,
                    succeed: true,
                    payload,
                    error_message: String::new(),
                    signature,
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

/// Builds the reply signature when the client asked for one. A `nonce` of 0
/// means "no signature requested" (proto3 default); legacy suppresses the reply
/// signature entirely in that case, including a remote-provided one.
///
/// When a signature is wanted, one already produced by a remote Dshackle
/// upstream (`provided`) wins and is passed through verbatim — keeping the
/// remote's `key_id`/`upstream_id` so the client verifies the instance closest
/// to the node. Local signing is the fallback; without a configured signer the
/// reply simply carries none. The reply's `nonce` is always the client's,
/// matching legacy `ctx.nonce`.
fn build_signature(
    signer: Option<&ResponseSigner>,
    nonce: u64,
    payload: &[u8],
    source: Option<UpstreamId>,
    provided: Option<ProvidedSignature>,
) -> Option<NativeCallReplySignature> {
    if nonce == 0 {
        return None;
    }
    let (signature, key_id, upstream_id) = if let Some(provided) = provided {
        (provided.value, provided.key_id, provided.upstream_id)
    } else {
        let signer = signer?;
        let Some(source) = source else {
            // Should not happen: every quorum records the source of the response
            // it resolves with. Better an unsigned reply than a wrong attribution.
            tracing::warn!("Response source unknown, skipping signature");
            return None;
        };
        let signature = signer.sign(nonce, payload, &source);
        (signature.value, signature.key_id, signature.upstream_id)
    };
    Some(NativeCallReplySignature {
        nonce,
        signature,
        key_id,
        upstream_id: upstream_id.to_string(),
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
    use crate::config::signature::{SignatureAlgorithm, SignatureConfig};
    use crate::upstream::label::UpstreamLabels;

    /// Local edge signer loaded from the shared signer testdata (key_id
    /// 0xd25f1ff2c1a57235), used to prove a remote signature outranks it.
    fn local_signer() -> ResponseSigner {
        let config = SignatureConfig {
            enabled: true,
            algorithm: SignatureAlgorithm::Secp256k1,
            private_key: Some("test_key".to_string()),
        };
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/signer");
        ResponseSigner::from_config(&config, &base)
            .unwrap()
            .unwrap()
    }

    fn remote_signature() -> ProvidedSignature {
        ProvidedSignature {
            value: vec![1, 2, 3],
            key_id: 0xAABB,
            upstream_id: "remote-node".parse().unwrap(),
        }
    }

    #[test]
    fn provided_signature_wins_over_local_signer() {
        // Chained deployment: even with its own key, the edge must forward the
        // remote's signature unchanged so the client verifies the remote's key.
        let signer = local_signer();
        let sig = build_signature(
            Some(&signer),
            7,
            b"\"0x1\"",
            Some("edge".parse().unwrap()),
            Some(remote_signature()),
        )
        .expect("provided signature is returned");
        assert_eq!(sig.key_id, 0xAABB);
        assert_eq!(sig.upstream_id, "remote-node");
        assert_eq!(sig.signature, vec![1, 2, 3]);
        // The reply carries the client's nonce, not whatever the remote echoed.
        assert_eq!(sig.nonce, 7);
    }

    #[test]
    fn provided_signature_used_without_local_signer() {
        // Edge without a signing key still passes the remote's signature through.
        let sig = build_signature(
            None,
            10,
            b"\"0x1\"",
            Some("edge".parse().unwrap()),
            Some(remote_signature()),
        )
        .expect("provided signature is returned");
        assert_eq!(sig.key_id, 0xAABB);
        assert_eq!(sig.upstream_id, "remote-node");
    }

    #[test]
    fn no_signature_for_provided_when_nonce_zero() {
        // Client didn't ask for a signature: legacy suppresses even a provided
        // one, so nonce==0 must win over the pass-through.
        assert!(
            build_signature(
                None,
                0,
                b"\"0x1\"",
                Some("edge".parse().unwrap()),
                Some(remote_signature())
            )
            .is_none()
        );
    }

    #[test]
    fn falls_back_to_local_signing_without_provided() {
        let signer = local_signer();
        let sig = build_signature(
            Some(&signer),
            10,
            b"\"0x1\"",
            Some("test-1".parse().unwrap()),
            None,
        )
        .expect("local signature is produced");
        assert_eq!(sig.key_id, 0xd25f1ff2c1a57235);
        assert_eq!(sig.upstream_id, "test-1");
        assert_eq!(sig.nonce, 10);
    }

    #[test]
    fn no_signature_when_nonce_zero() {
        let signer = local_signer();
        assert!(
            build_signature(
                Some(&signer),
                0,
                b"\"0x1\"",
                Some("test-1".parse().unwrap()),
                None
            )
            .is_none()
        );
    }

    #[test]
    fn no_signature_without_signer_or_provided() {
        assert!(
            build_signature(None, 10, b"\"0x1\"", Some("test-1".parse().unwrap()), None).is_none()
        );
    }

    // ── Null-result pass-through (whole call path) ────────────────────────

    use crate::jsonrpc::JsonRpcResponse;
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::quorum::{AlwaysQuorum, CallQuorum, QuorumFactory};
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::RpcUpstream;
    use std::sync::Arc;

    /// Upstream that returns a null result (no `result`, no `error`) already
    /// carrying a remote-provided signature — the shape a remote Dshackle sends
    /// for a signed request whose result is JSON `null`.
    struct NullWithProvidedSignature {
        state: Arc<UpstreamState>,
    }

    #[async_trait::async_trait]
    impl RpcUpstream for NullWithProvidedSignature {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            Ok(JsonRpcResponse {
                id: serde_json::Value::from(1),
                result: None,
                error: None,
                provided_signature: Some(ProvidedSignature {
                    value: vec![9, 9, 9],
                    key_id: 0xCAFE,
                    upstream_id: "remote-node".parse().unwrap(),
                }),
            })
        }
        fn id(&self) -> &UpstreamId {
            static ID: std::sync::LazyLock<UpstreamId> =
                std::sync::LazyLock::new(|| "edge".parse().unwrap());
            &ID
        }
        fn availability(&self) -> UpstreamAvailability {
            UpstreamAvailability::Ok
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            None
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
    }

    fn test_chain() -> crate::blockchain::TargetBlockchain {
        crate::blockchain::TargetBlockchain::Standard(
            emerald_api::proto::common::ChainRef::ChainEthereum,
        )
    }

    struct AlwaysFactory;
    impl QuorumFactory for AlwaysFactory {
        fn quorum_for(&self, _method: &crate::jsonrpc::RpcMethod) -> Box<dyn CallQuorum> {
            Box::new(AlwaysQuorum::new())
        }
    }

    #[tokio::test]
    async fn null_result_still_passes_through_provided_signature() {
        let upstream = Arc::new(NullWithProvidedSignature {
            state: Arc::new(UpstreamState::new()),
        });
        let ms = Multistream::new(test_chain(), vec![upstream], Arc::new(AlwaysFactory));
        let item = NativeCallItem {
            id: 1,
            method: "eth_getTransactionByHash".to_string(),
            payload: b"[]".to_vec(),
            nonce: 5,
        };

        let reply = execute_native_call(&ms, &item, &LabelSelector::Any, None).await;

        assert!(reply.succeed);
        assert_eq!(reply.payload, b"null");
        let sig = reply.signature.expect(
            "a null result the client asked to sign must still carry the provided signature",
        );
        assert_eq!(sig.key_id, 0xCAFE);
        assert_eq!(sig.upstream_id, "remote-node");
        // Reply nonce is the client's, not the remote's echo.
        assert_eq!(sig.nonce, 5);
    }

    // ── Label- and height-constrained routing ─────────────────────────────

    use crate::upstream::head::CurrentHead;

    /// Upstream with fixed labels, head height, and availability, answering
    /// every call with its own id — so tests can see who served the request.
    struct LabeledUpstream {
        label: UpstreamId,
        label_sets: Vec<UpstreamLabels>,
        head: CurrentHead,
        availability: UpstreamAvailability,
        state: Arc<UpstreamState>,
    }

    impl LabeledUpstream {
        fn new(id: &str, labels: &[(&str, &str)], height: Option<u64>) -> Arc<Self> {
            Self::with_availability(id, labels, height, UpstreamAvailability::Ok)
        }

        fn with_availability(
            id: &str,
            labels: &[(&str, &str)],
            height: Option<u64>,
            availability: UpstreamAvailability,
        ) -> Arc<Self> {
            let head = CurrentHead::new();
            if let Some(h) = height {
                head.update(h);
            }
            Arc::new(Self {
                label: id.parse().unwrap(),
                label_sets: vec![crate::upstream::label::test_labels(labels)],
                head,
                availability,
                state: Arc::new(UpstreamState::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for LabeledUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{}"}}"#, self.label);
            Ok(serde_json::from_str(&body).unwrap())
        }
        fn id(&self) -> &UpstreamId {
            &self.label
        }
        fn availability(&self) -> UpstreamAvailability {
            self.availability
        }
        fn head(&self) -> &dyn Head {
            &self.head
        }
        fn lag(&self) -> Option<u64> {
            Some(0)
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
        fn label_sets(&self) -> &[UpstreamLabels] {
            &self.label_sets
        }
    }

    fn result_of(routed: &Routed) -> String {
        routed
            .response
            .result
            .as_ref()
            .unwrap()
            .get()
            .trim_matches('"')
            .to_string()
    }

    #[tokio::test]
    async fn label_selector_routes_to_matching_upstream() {
        let ms = Multistream::new(
            test_chain(),
            vec![
                LabeledUpstream::new("plain", &[("archive", "false")], Some(100)),
                LabeledUpstream::new("archive", &[("archive", "true")], Some(100)),
            ],
            Arc::new(AlwaysFactory),
        );
        let request = JsonRpcRequest::new(1, "eth_blockNumber".into(), serde_json::json!([]));
        let selector = LabelSelector::Label {
            name: "archive".into(),
            values: vec!["true".into()],
        };

        // Repeated calls advance the round-robin cursor; the label filter must
        // hold regardless of which upstream leads.
        for _ in 0..4 {
            let routed = execute_call(&ms, &request, &selector).await.unwrap();
            assert_eq!(result_of(&routed), "archive");
        }
    }

    #[tokio::test]
    async fn label_selector_with_no_match_fails() {
        let ms = Multistream::new(
            test_chain(),
            vec![LabeledUpstream::new(
                "plain",
                &[("archive", "false")],
                Some(100),
            )],
            Arc::new(AlwaysFactory),
        );
        let request = JsonRpcRequest::new(1, "eth_blockNumber".into(), serde_json::json!([]));
        let selector = LabelSelector::Exists("special".into());

        let err = execute_call(&ms, &request, &selector).await.unwrap_err();
        assert!(matches!(err, UpstreamError::Transport(_)));
    }

    #[tokio::test]
    async fn historic_read_skips_upstreams_below_the_block() {
        let ms = Multistream::new(
            test_chain(),
            vec![
                LabeledUpstream::new("pruned", &[], Some(50)),
                LabeledUpstream::new("full", &[], Some(200)),
            ],
            Arc::new(AlwaysFactory),
        );
        // eth_getBalance at block 0x64 (=100): only "full" has reached it.
        let request = JsonRpcRequest::new(
            1,
            "eth_getBalance".into(),
            serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "0x64"]),
        );

        for _ in 0..4 {
            let routed = execute_call(&ms, &request, &LabelSelector::Any)
                .await
                .unwrap();
            assert_eq!(result_of(&routed), "full");
        }
    }

    #[tokio::test]
    async fn latest_read_survives_an_unavailable_head_most_upstream() {
        // "sick" once reported the highest head, then went unavailable — its
        // height must not become the "latest" bar, or the healthy upstream
        // one block behind would be filtered out and the whole chain errors.
        let sick = LabeledUpstream::with_availability(
            "sick",
            &[],
            Some(1005),
            UpstreamAvailability::Unavailable,
        );
        let healthy = LabeledUpstream::new("healthy", &[], Some(1004));
        let ms = Multistream::new(test_chain(), vec![sick, healthy], Arc::new(AlwaysFactory));

        let request = JsonRpcRequest::new(
            1,
            "eth_getBalance".into(),
            serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "latest"]),
        );
        let routed = execute_call(&ms, &request, &LabelSelector::Any)
            .await
            .unwrap();
        assert_eq!(result_of(&routed), "healthy");
    }

    #[tokio::test]
    async fn pinned_read_routes_during_startup_before_heads_are_known() {
        // Right after a restart no upstream has reported a head yet; a pinned
        // read must route best-effort instead of deterministically failing on
        // every deploy.
        let a = LabeledUpstream::new("up-a", &[], None);
        let ms = Multistream::new(test_chain(), vec![a], Arc::new(AlwaysFactory));

        let request = JsonRpcRequest::new(
            1,
            "eth_getBalance".into(),
            serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "0x64"]),
        );
        let routed = execute_call(&ms, &request, &LabelSelector::Any)
            .await
            .unwrap();
        assert_eq!(result_of(&routed), "up-a");
    }

    #[tokio::test]
    async fn historic_read_fails_when_no_upstream_has_the_block() {
        // Routing a historic read to a node without the block would return a
        // wrong (empty) answer, so unlike other filters this must fail hard.
        let ms = Multistream::new(
            test_chain(),
            vec![LabeledUpstream::new("pruned", &[], Some(50))],
            Arc::new(AlwaysFactory),
        );
        let request = JsonRpcRequest::new(
            1,
            "eth_getBalance".into(),
            serde_json::json!(["0x690b2bdf41f33f9f251ae0459e5898b856ed96be", "0x64"]),
        );

        let err = execute_call(&ms, &request, &LabelSelector::Any)
            .await
            .unwrap_err();
        assert!(matches!(err, UpstreamError::Transport(_)));
    }

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
