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

//! Signed response configuration.

use serde::Deserialize;

/// Configuration for cryptographically signing responses.
#[derive(Debug, Clone, Deserialize)]
pub struct SignatureConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub algorithm: SignatureAlgorithm,
    #[serde(alias = "privateKey", rename = "private-key")]
    pub private_key: Option<String>,
}

/// Supported signature algorithms.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub enum SignatureAlgorithm {
    #[default]
    #[serde(rename = "SECP256K1", alias = "secp256k1")]
    Secp256k1,
    #[serde(rename = "NIST-P256", alias = "nist-p256", alias = "NIST_P256")]
    NistP256,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_enabled_signature() {
        let yaml = r#"
signed-response:
  enabled: true
  algorithm: SECP256K1
  private-key: /root/key.pem
"#;
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(rename = "signed-response")]
            sig: SignatureConfig,
        }
        let w: Wrapper = serde_yaml::from_str(yaml).unwrap();
        assert!(w.sig.enabled);
        assert_eq!(w.sig.algorithm, SignatureAlgorithm::Secp256k1);
        assert_eq!(w.sig.private_key.as_deref(), Some("/root/key.pem"));
    }

    #[test]
    fn parse_disabled_signature() {
        let yaml = r#"
signed-response:
  enabled: false
  private-key: /root/key.pem
"#;
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(rename = "signed-response")]
            sig: SignatureConfig,
        }
        let w: Wrapper = serde_yaml::from_str(yaml).unwrap();
        assert!(!w.sig.enabled);
    }
}
