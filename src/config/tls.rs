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

use percent_encoding::percent_decode_str;
use serde::Deserialize;

fn decode_userinfo_component(component: &str) -> anyhow::Result<String> {
    percent_decode_str(component)
        .decode_utf8()
        .map(|s| s.into_owned())
        .map_err(|_| anyhow::anyhow!("invalid percent-encoding in upstream URL credentials"))
}

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
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

/// If `url` carries HTTP basic credentials in the userinfo (`user:pass@host`),
/// returns the same URL with the userinfo removed and those credentials.
/// Percent-encoded usernames and passwords are decoded. When there is no
/// userinfo, returns the input URL unchanged and `None`.
pub fn basic_auth_from_url(url: &str) -> anyhow::Result<(String, Option<BasicAuth>)> {
    let mut parsed = url::Url::parse(url)?;
    if parsed.username().is_empty() && parsed.password().is_none() {
        return Ok((url.to_string(), None));
    }

    let auth = BasicAuth {
        username: decode_userinfo_component(parsed.username())?,
        password: decode_userinfo_component(parsed.password().unwrap_or_default())?,
    };
    parsed
        .set_username("")
        .map_err(|_| anyhow::anyhow!("failed to strip username from upstream URL"))?;
    parsed
        .set_password(None)
        .map_err(|_| anyhow::anyhow!("failed to strip password from upstream URL"))?;
    Ok((parsed.to_string(), Some(auth)))
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
    fn basic_auth_from_url_decodes_userinfo() {
        let (url, auth) =
            basic_auth_from_url("https://user:pass@rpc.example.com/v1").unwrap();
        assert_eq!(url, "https://rpc.example.com/v1");
        let auth = auth.unwrap();
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "pass");
    }

    #[test]
    fn basic_auth_from_url_decodes_percent_encoded_userinfo() {
        let (url, auth) =
            basic_auth_from_url("https://us%40er:p%40ss@rpc.example.com/v1").unwrap();
        assert_eq!(url, "https://rpc.example.com/v1");
        let auth = auth.unwrap();
        assert_eq!(auth.username, "us@er");
        assert_eq!(auth.password, "p@ss");
    }

    #[test]
    fn basic_auth_from_url_leaves_plain_urls_alone() {
        let (url, auth) = basic_auth_from_url("http://localhost:8545").unwrap();
        assert_eq!(url, "http://localhost:8545");
        assert!(auth.is_none());
    }

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
