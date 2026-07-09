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

//! Builds the server TLS setup from the parsed configuration, shared by the
//! gRPC server and the JSON-RPC HTTP proxy. Each server maps the loaded
//! material onto its own stack (tonic, warp).

use crate::config::tls::{ServerTlsConfig, TlsClientRequirement};
use anyhow::{bail, Context, Result};
use std::path::Path;
use tracing;

/// TLS material for one server socket: PEM contents loaded from the configured
/// files, plus the client-authentication mode.
pub struct ServerTlsSetup {
    pub certificate: Vec<u8>,
    pub key: Vec<u8>,
    pub client: ClientAuth,
}

impl ServerTlsSetup {
    /// Builds a rustls server config for servers that terminate TLS
    /// themselves (the JSON-RPC proxy). The gRPC server maps the same
    /// material through tonic instead.
    pub fn rustls_config(&self) -> Result<rustls::ServerConfig> {
        let certs = rustls_pemfile::certs(&mut self.certificate.as_slice())
            .collect::<Result<Vec<_>, _>>()
            .context("parsing TLS certificate")?;
        let key = rustls_pemfile::private_key(&mut self.key.as_slice())
            .context("parsing TLS key")?
            .ok_or_else(|| anyhow::anyhow!("no private key found in the TLS key file"))?;

        let builder = rustls::ServerConfig::builder();
        let builder = match &self.client {
            ClientAuth::TrustAll => builder.with_no_client_auth(),
            ClientAuth::Optional(ca) => {
                builder.with_client_cert_verifier(client_verifier(ca, true)?)
            }
            ClientAuth::Required(ca) => {
                builder.with_client_cert_verifier(client_verifier(ca, false)?)
            }
        };
        builder
            .with_single_cert(certs, key)
            .context("invalid TLS certificate or key")
    }
}

/// Builds a client-certificate verifier from a CA bundle. An "optional"
/// verifier still validates a certificate when one is presented — it only
/// stops requiring one, same as the gRPC side.
fn client_verifier(
    ca: &[u8],
    allow_unauthenticated: bool,
) -> Result<std::sync::Arc<dyn rustls::server::danger::ClientCertVerifier>> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut &ca[..]) {
        roots.add(cert.context("parsing client CA")?)?;
    }
    let builder = rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(roots));
    let builder = if allow_unauthenticated {
        builder.allow_unauthenticated()
    } else {
        builder
    };
    builder
        .build()
        .context("building client certificate verifier")
}

/// Decides whether a server must run with TLS and, if so, loads the
/// certificates.
///
/// Follows the legacy JVM decision logic: TLS is on when explicitly enabled or
/// when a server certificate is configured, and explicitly `enabled: false`
/// always wins. Missing certificate with `enabled: true` is a startup error.
///
/// `category` only labels the log messages, matching the legacy output
/// (e.g. "Native gRPC", "proxy"). Relative certificate paths are resolved
/// against `base_dir`, the directory of the config file.
pub fn server_tls(
    category: &str,
    config: Option<&ServerTlsConfig>,
    base_dir: &Path,
) -> Result<Option<ServerTlsSetup>> {
    let Some(config) = config.filter(|c| c.is_enabled()) else {
        tracing::warn!("Using insecure transport for {category}");
        return Ok(None);
    };
    let Some(server) = &config.server else {
        bail!("tls.server.certificate property for {category} is not set (path to server TLS certificate) but TLS is enabled");
    };

    tracing::info!("Using TLS for {category}");
    let certificate = read_file(base_dir, &server.certificate)
        .with_context(|| format!("reading TLS certificate for {category}"))?;
    let key = read_file(base_dir, &server.key)
        .with_context(|| format!("reading TLS certificate key for {category}"))?;
    let client = client_auth(category, config.client.as_ref(), base_dir)?;

    Ok(Some(ServerTlsSetup {
        certificate,
        key,
        client,
    }))
}

/// How the server treats client certificates, decided from the `tls.client`
/// config section.
#[derive(Debug, PartialEq)]
pub enum ClientAuth {
    /// No CA configured: any client may connect, certificates are not requested.
    TrustAll,
    /// CA configured without `require`: a certificate is validated when
    /// presented, but connections without one are still accepted.
    Optional(Vec<u8>),
    /// CA configured with `require: true`: no valid certificate — no connection.
    Required(Vec<u8>),
}

fn client_auth(
    category: &str,
    client: Option<&TlsClientRequirement>,
    base_dir: &Path,
) -> Result<ClientAuth> {
    match client {
        Some(TlsClientRequirement {
            ca: Some(ca_path),
            require,
        }) => {
            tracing::info!("Using TLS for client authentication for {category}");
            let ca = read_file(base_dir, ca_path)
                .with_context(|| format!("reading client CA for {category}"))?;
            if *require {
                Ok(ClientAuth::Required(ca))
            } else {
                Ok(ClientAuth::Optional(ca))
            }
        }
        Some(TlsClientRequirement {
            require: true,
            ca: None,
        }) => {
            bail!("Client Certificate not set for {category}: tls.client.require is enabled but tls.client.ca is missing");
        }
        _ => {
            tracing::warn!("Trust all clients for {category}");
            Ok(ClientAuth::TrustAll)
        }
    }
}

/// Reads a file resolving a relative path against the config file directory,
/// as the legacy FileResolver did.
fn read_file(base_dir: &Path, path: &str) -> Result<Vec<u8>> {
    let resolved = crate::config::resolve_file(base_dir, path);
    std::fs::read(&resolved).with_context(|| format!("reading {}", resolved.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::tls::{TlsClientRequirement, TlsServerCert};

    fn server_cert(dir: &Path) -> TlsServerCert {
        std::fs::write(dir.join("server.crt"), "cert").unwrap();
        std::fs::write(dir.join("server.key"), "key").unwrap();
        TlsServerCert {
            certificate: "server.crt".to_string(),
            key: "server.key".to_string(),
        }
    }

    #[test]
    fn no_config_is_insecure() {
        let result = server_tls("test", None, Path::new(".")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn disabled_ignores_certificate() {
        let dir = tempfile::tempdir().unwrap();
        let config = ServerTlsConfig {
            enabled: Some(false),
            server: Some(server_cert(dir.path())),
            client: None,
        };
        let result = server_tls("test", Some(&config), dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn enabled_without_certificate_fails() {
        let config = ServerTlsConfig {
            enabled: Some(true),
            server: None,
            client: None,
        };
        let result = server_tls("test", Some(&config), Path::new("."));
        assert!(result.is_err());
    }

    #[test]
    fn certificate_enables_tls_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let config = ServerTlsConfig {
            enabled: None,
            server: Some(server_cert(dir.path())),
            client: None,
        };
        let result = server_tls("test", Some(&config), dir.path()).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn missing_certificate_file_fails() {
        let dir = tempfile::tempdir().unwrap();
        let config = ServerTlsConfig {
            enabled: Some(true),
            server: Some(TlsServerCert {
                certificate: "absent.crt".to_string(),
                key: "absent.key".to_string(),
            }),
            client: None,
        };
        let result = server_tls("test", Some(&config), dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn client_auth_with_ca() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("ca.crt"), "ca").unwrap();
        let config = ServerTlsConfig {
            enabled: None,
            server: Some(server_cert(dir.path())),
            client: Some(TlsClientRequirement {
                require: true,
                ca: Some("ca.crt".to_string()),
            }),
        };
        let result = server_tls("test", Some(&config), dir.path()).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn client_auth_optional_when_not_required() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("ca.crt"), "ca").unwrap();
        let client = TlsClientRequirement {
            require: false,
            ca: Some("ca.crt".to_string()),
        };
        let result = client_auth("test", Some(&client), dir.path()).unwrap();
        assert_eq!(result, ClientAuth::Optional(b"ca".to_vec()));
    }

    #[test]
    fn client_auth_required_with_ca() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("ca.crt"), "ca").unwrap();
        let client = TlsClientRequirement {
            require: true,
            ca: Some("ca.crt".to_string()),
        };
        let result = client_auth("test", Some(&client), dir.path()).unwrap();
        assert_eq!(result, ClientAuth::Required(b"ca".to_vec()));
    }

    #[test]
    fn client_auth_trusts_all_without_ca() {
        let result = client_auth("test", None, Path::new(".")).unwrap();
        assert_eq!(result, ClientAuth::TrustAll);

        let client = TlsClientRequirement {
            require: false,
            ca: None,
        };
        let result = client_auth("test", Some(&client), Path::new(".")).unwrap();
        assert_eq!(result, ClientAuth::TrustAll);
    }

    #[test]
    fn required_client_auth_without_ca_fails() {
        let dir = tempfile::tempdir().unwrap();
        let config = ServerTlsConfig {
            enabled: None,
            server: Some(server_cert(dir.path())),
            client: Some(TlsClientRequirement {
                require: true,
                ca: None,
            }),
        };
        let result = server_tls("test", Some(&config), dir.path());
        assert!(result.is_err());
    }
}
