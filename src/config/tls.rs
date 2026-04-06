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

//! TLS and authentication configuration types.

use serde::Deserialize;

/// Server-side TLS configuration as it appears in YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerTlsConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    pub server: Option<TlsServerCert>,
    pub client: Option<TlsClientRequirement>,
}

impl ServerTlsConfig {
    /// Returns true if TLS is explicitly enabled or has any configuration set.
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(self.server.is_some())
    }
}

/// Server certificate and key paths.
#[derive(Debug, Clone, Deserialize)]
pub struct TlsServerCert {
    pub certificate: String,
    pub key: String,
}

/// Client authentication requirements for the server.
#[derive(Debug, Clone, Deserialize)]
pub struct TlsClientRequirement {
    #[serde(default)]
    pub require: bool,
    pub ca: Option<String>,
}

/// Client-side basic authentication credentials.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

/// Client-side TLS authentication (used for gRPC and upstream connections).
#[derive(Debug, Clone, Deserialize)]
pub struct ClientTlsAuth {
    pub ca: Option<String>,
    pub certificate: Option<String>,
    pub key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_auth() {
        let yaml = r#"
basic-auth:
  username: 9c199ad8f281f20154fc258fe41a6814
  password: 258fe4149c199ad8f2811a68f20154fc
"#;
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(rename = "basic-auth")]
            basic_auth: BasicAuth,
        }
        let w: Wrapper = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(w.basic_auth.username, "9c199ad8f281f20154fc258fe41a6814");
        assert_eq!(w.basic_auth.password, "258fe4149c199ad8f2811a68f20154fc");
    }

    #[test]
    fn parse_client_tls() {
        let yaml = r#"
tls:
  ca: /etc/ca.myservice.com.crt
  certificate: /etc/client1.myservice.com.crt
  key: /etc/client1.myservice.com.key
"#;
        #[derive(Deserialize)]
        struct Wrapper {
            tls: ClientTlsAuth,
        }
        let w: Wrapper = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(w.tls.ca.as_deref(), Some("/etc/ca.myservice.com.crt"));
        assert_eq!(
            w.tls.certificate.as_deref(),
            Some("/etc/client1.myservice.com.crt")
        );
        assert_eq!(w.tls.key.as_deref(), Some("/etc/client1.myservice.com.key"));
    }

    #[test]
    fn parse_server_tls() {
        let yaml = r#"
enabled: true
server:
  certificate: "/etc/client1.myservice.com.crt"
  key: "/etc/client1.myservice.com.key"
client:
  require: false
  ca: /etc/ca.myservice.com.crt
"#;
        let tls: ServerTlsConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(tls.is_enabled());
        assert_eq!(tls.enabled, Some(true));
        let server = tls.server.unwrap();
        assert_eq!(server.certificate, "/etc/client1.myservice.com.crt");
        assert_eq!(server.key, "/etc/client1.myservice.com.key");
        let client = tls.client.unwrap();
        assert!(!client.require);
        assert_eq!(client.ca.as_deref(), Some("/etc/ca.myservice.com.crt"));
    }
}
