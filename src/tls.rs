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

//! Builds the gRPC server TLS setup from the parsed configuration.

use crate::config::tls::{ServerTlsConfig, TlsClientRequirement};
use anyhow::{bail, Context, Result};
use std::path::Path;
use tonic::transport::server::ServerTlsConfig as TonicServerTls;
use tonic::transport::{Certificate, Identity};
use tracing;

/// Decides whether the gRPC server must run with TLS and, if so, loads the
/// certificates and builds the tonic TLS config.
///
/// Follows the legacy JVM decision logic: TLS is on when explicitly enabled or
/// when a server certificate is configured, and explicitly `enabled: false`
/// always wins. Missing certificate with `enabled: true` is a startup error.
///
/// `category` only labels the log messages, matching the legacy output
/// (e.g. "Native gRPC"). Relative certificate paths are resolved against
/// `base_dir`, the directory of the config file.
pub fn grpc_server_tls(
    category: &str,
    config: Option<&ServerTlsConfig>,
    base_dir: &Path,
) -> Result<Option<TonicServerTls>> {
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
    let mut tls = TonicServerTls::new().identity(Identity::from_pem(certificate, key));

    match client_auth(category, config.client.as_ref(), base_dir)? {
        ClientAuth::TrustAll => {}
        ClientAuth::Optional(ca) => {
            tls = tls
                .client_ca_root(Certificate::from_pem(ca))
                .client_auth_optional(true);
        }
        ClientAuth::Required(ca) => {
            tls = tls
                .client_ca_root(Certificate::from_pem(ca))
                .client_auth_optional(false);
        }
    }

    Ok(Some(tls))
}

/// How the server treats client certificates, decided from the `tls.client`
/// config section.
#[derive(Debug, PartialEq)]
enum ClientAuth {
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
        let result = grpc_server_tls("test", None, Path::new(".")).unwrap();
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
        let result = grpc_server_tls("test", Some(&config), dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn enabled_without_certificate_fails() {
        let config = ServerTlsConfig {
            enabled: Some(true),
            server: None,
            client: None,
        };
        let result = grpc_server_tls("test", Some(&config), Path::new("."));
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
        let result = grpc_server_tls("test", Some(&config), dir.path()).unwrap();
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
        let result = grpc_server_tls("test", Some(&config), dir.path());
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
        let result = grpc_server_tls("test", Some(&config), dir.path()).unwrap();
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
        let result = grpc_server_tls("test", Some(&config), dir.path());
        assert!(result.is_err());
    }
}
