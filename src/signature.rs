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

//! Signs `NativeCall` responses so a client can verify which key (and which
//! upstream) produced a result. Port of the legacy `EcdsaSigner`.

use crate::config::signature::{SignatureAlgorithm, SignatureConfig};
use anyhow::{Context, Result, bail};
use k256::ecdsa::signature::Signer as _;
use k256::pkcs8::{DecodePrivateKey, EncodePublicKey};
use sha2::{Digest, Sha256};
use std::path::Path;
use tracing;

/// Tag prefixing every signed message, preventing the signature from being
/// valid in any other context.
const MSG_PREFIX: &str = "DSHACKLESIG";

/// Signs response payloads with the configured ECDSA key.
pub struct ResponseSigner {
    key: SignerKey,
    key_id: u64,
}

enum SignerKey {
    Secp256k1(k256::ecdsa::SigningKey),
    NistP256(p256::ecdsa::SigningKey),
}

/// A signature received from a remote Dshackle upstream, to be passed through
/// to the client unchanged.
///
/// In a chained deployment (edge → remote Dshackle → node) the edge forwards
/// the client's nonce, and the remote — sitting closest to the node — signs the
/// result. Passing that signature through instead of re-signing locally lets the
/// client verify the instance nearest the node, so an intermediate edge cannot
/// tamper with the payload. Port of the legacy `JsonRpcResponse.providedSignature`.
///
/// Carries no nonce: like legacy `ResponseSigner.Signature`, the reply's nonce
/// is always the client's own, not whatever the remote echoed back.
#[derive(Debug, Clone)]
pub struct ProvidedSignature {
    /// DER-encoded ECDSA signature bytes.
    pub value: Vec<u8>,
    /// Identifies the remote's signing key (its own `key_id`).
    pub key_id: u64,
    /// The upstream on the remote that produced the signed response.
    pub upstream_id: String,
}

/// A produced signature together with the identifiers a client needs to
/// verify it.
pub struct ResponseSignature {
    /// DER-encoded ECDSA signature over the wrapped message.
    pub value: Vec<u8>,
    /// Identifies the signing key: first 8 bytes (big-endian) of
    /// SHA-256 over the DER-encoded public key.
    pub key_id: u64,
    /// The upstream that produced the signed response.
    pub upstream_id: String,
}

impl ResponseSigner {
    /// Creates the signer from the `signed-response` config section, loading
    /// the PKCS#8 PEM key. Returns `None` when signing is not enabled;
    /// enabled without a valid key is a startup error.
    pub fn from_config(config: &SignatureConfig, base_dir: &Path) -> Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }
        let Some(key_path) = &config.private_key else {
            bail!("signed-response is enabled but signed-response.private-key is not set");
        };
        let resolved = crate::config::resolve_file(base_dir, key_path);
        let pem = std::fs::read_to_string(&resolved)
            .with_context(|| format!("reading signing key {}", resolved.display()))?;

        // A parse failure here is either a malformed key or a key on the
        // wrong curve (the PKCS#8 header names the curve), matching the
        // legacy per-curve validation.
        let (key, public_der) = match config.algorithm {
            SignatureAlgorithm::Secp256k1 => {
                let key = k256::ecdsa::SigningKey::from_pkcs8_pem(&pem).with_context(|| {
                    format!(
                        "loading {} as a SECP256K1 key — generate SECP256K1 or configure another algorithm",
                        resolved.display()
                    )
                })?;
                let public = key
                    .verifying_key()
                    .to_public_key_der()
                    .context("encoding public key")?;
                (SignerKey::Secp256k1(key), public)
            }
            SignatureAlgorithm::NistP256 => {
                let key = p256::ecdsa::SigningKey::from_pkcs8_pem(&pem).with_context(|| {
                    format!(
                        "loading {} as a NIST P-256 key — generate NIST P-256 or configure another algorithm",
                        resolved.display()
                    )
                })?;
                let public = key
                    .verifying_key()
                    .to_public_key_der()
                    .context("encoding public key")?;
                (SignerKey::NistP256(key), public)
            }
        };

        let full_id = Sha256::digest(public_der.as_bytes());
        tracing::info!(
            "Using key to sign responses: {}",
            hex::encode(&full_id[..8])
        );
        let key_id = u64::from_be_bytes(full_id[..8].try_into().expect("sha256 is 32 bytes"));

        Ok(Some(Self { key, key_id }))
    }

    /// Signs one response payload for the given request nonce.
    ///
    /// Refuses (`None`) an upstream id containing `/` — that's the separator
    /// of the wrapped message, so such an id could forge the other fields.
    /// Legacy rejects those ids at config read; the Rust config does not yet,
    /// so the signer is the last line of defense.
    pub fn sign(&self, nonce: u64, message: &[u8], upstream_id: &str) -> Option<ResponseSignature> {
        if upstream_id.contains('/') {
            tracing::warn!("Upstream id '{upstream_id}' contains '/', refusing to sign");
            return None;
        }
        let wrapped = wrap_message(nonce, message, upstream_id);
        let value = match &self.key {
            SignerKey::Secp256k1(key) => {
                let sig: k256::ecdsa::Signature = key.sign(wrapped.as_bytes());
                sig.to_der().as_bytes().to_vec()
            }
            SignerKey::NistP256(key) => {
                let sig: p256::ecdsa::Signature = key.sign(wrapped.as_bytes());
                sig.to_der().as_bytes().to_vec()
            }
        };
        Some(ResponseSignature {
            value,
            key_id: self.key_id,
            upstream_id: upstream_id.to_string(),
        })
    }
}

/// Wraps the payload for signing as
/// `DSHACKLESIG/<nonce>/<upstream_id>/<hex(sha256(message))>`, so no part of
/// the message can bleed into another and the signature is bound to its
/// source. Must stay byte-identical to the legacy `EcdsaSigner.wrapMessage`.
fn wrap_message(nonce: u64, message: &[u8], upstream_id: &str) -> String {
    format!(
        "{MSG_PREFIX}/{nonce}/{upstream_id}/{}",
        hex::encode(Sha256::digest(message))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::signature::Verifier as _;

    fn test_config() -> SignatureConfig {
        SignatureConfig {
            enabled: true,
            algorithm: SignatureAlgorithm::Secp256k1,
            private_key: Some("test_key".to_string()),
        }
    }

    fn testdata() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/signer")
    }

    #[test]
    fn disabled_config_creates_no_signer() {
        let config = SignatureConfig {
            enabled: false,
            ..test_config()
        };
        let signer = ResponseSigner::from_config(&config, &testdata()).unwrap();
        assert!(signer.is_none());
    }

    #[test]
    fn enabled_without_key_fails() {
        let config = SignatureConfig {
            private_key: None,
            ..test_config()
        };
        let result = ResponseSigner::from_config(&config, &testdata());
        assert!(result.is_err());
    }

    #[test]
    fn wrong_algorithm_for_key_fails() {
        let config = SignatureConfig {
            algorithm: SignatureAlgorithm::NistP256,
            ..test_config()
        };
        let result = ResponseSigner::from_config(&config, &testdata());
        assert!(result.is_err());
    }

    #[test]
    fn wraps_message_in_legacy_format() {
        // sha256("\"0x100001\"") over the raw JSON result bytes
        let wrapped = wrap_message(10, b"\"0x100001\"", "test-1");
        let hash = hex::encode(Sha256::digest(b"\"0x100001\""));
        assert_eq!(wrapped, format!("DSHACKLESIG/10/test-1/{hash}"));
    }

    #[test]
    fn wraps_message_matching_legacy_test_vector() {
        // Test vector from the legacy EcdsaSignerSpec."Wrap message"
        let wrapped = wrap_message(10, b"test", "infura");
        assert_eq!(
            wrapped,
            "DSHACKLESIG/10/infura/9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }

    #[test]
    fn key_id_matches_legacy_test_vector() {
        // Test vector from the legacy EcdsaSignerSpec."Id is a hash of x509
        // public key", for the same testdata/signer/test_key. Proves that the
        // public key DER encoding is byte-identical to the JVM's
        // `publicKey.getEncoded()`.
        let signer = ResponseSigner::from_config(&test_config(), &testdata())
            .unwrap()
            .unwrap();
        assert_eq!(signer.key_id, 0xd25f1ff2c1a57235);
    }

    #[test]
    fn refuses_upstream_id_with_slash() {
        let signer = ResponseSigner::from_config(&test_config(), &testdata())
            .unwrap()
            .unwrap();
        assert!(signer.sign(10, b"\"0x100001\"", "bad/id").is_none());
    }

    #[test]
    fn signature_verifies_with_public_key() {
        let signer = ResponseSigner::from_config(&test_config(), &testdata())
            .unwrap()
            .unwrap();
        let signature = signer.sign(10, b"\"0x100001\"", "test-1").unwrap();
        assert_eq!(signature.upstream_id, "test-1");
        assert_eq!(signature.key_id, signer.key_id);

        let public_pem = std::fs::read_to_string(testdata().join("test_key.pub")).unwrap();
        let verifying_key =
            <k256::ecdsa::VerifyingKey as k256::pkcs8::DecodePublicKey>::from_public_key_pem(
                &public_pem,
            )
            .unwrap();
        let parsed = k256::ecdsa::Signature::from_der(&signature.value).unwrap();
        let wrapped = wrap_message(10, b"\"0x100001\"", "test-1");
        verifying_key
            .verify(wrapped.as_bytes(), &parsed)
            .expect("signature must verify");
    }
}
