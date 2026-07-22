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

//! Builds TLS setups from the parsed configuration: server-side (shared by the
//! gRPC server and the JSON-RPC HTTP proxy) and client-side (outgoing upstream
//! connections). Each consumer maps the loaded material onto its own stack
//! (tonic, warp, reqwest).

use crate::config::tls::{ClientTlsAuth, ServerTlsConfig, TlsClientRequirement};
use anyhow::{Context, Result, bail};
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
        bail!(
            "tls.server.certificate property for {category} is not set (path to server TLS certificate) but TLS is enabled"
        );
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
            bail!(
                "Client Certificate not set for {category}: tls.client.require is enabled but tls.client.ca is missing"
            );
        }
        _ => {
            tracing::warn!("Trust all clients for {category}");
            Ok(ClientAuth::TrustAll)
        }
    }
}

/// Client-side TLS material for one outgoing upstream connection.
///
/// Present at all means "use TLS": with no fields set the connection uses the
/// system trust roots (the legacy `auto-tls`); a CA narrows trust to that CA
/// alone; a certificate/key pair adds mutual TLS.
pub struct ClientTlsSetup {
    /// Custom CA that replaces the system roots, matching the legacy
    /// `trustManager(ca)` behavior of trusting only the configured CA.
    pub ca: Option<Vec<u8>>,
    /// Client certificate and key for mutual TLS.
    pub identity: Option<ClientIdentity>,
}

/// A client certificate with its private key, as PEM bytes.
pub struct ClientIdentity {
    pub certificate: Vec<u8>,
    pub key: Vec<u8>,
}

/// Loads the client TLS material for an outgoing connection.
///
/// `target` labels errors and logs (e.g. the upstream id). A certificate
/// without a key (or vice versa) is a startup error rather than the legacy
/// fallback of silently connecting with the CA only.
pub fn client_tls(
    target: &str,
    config: Option<&ClientTlsAuth>,
    base_dir: &Path,
) -> Result<Option<ClientTlsSetup>> {
    let Some(config) = config else {
        return Ok(None);
    };

    let ca = match &config.ca {
        Some(path) => Some(
            read_file(base_dir, path).with_context(|| format!("reading TLS CA for {target}"))?,
        ),
        None => None,
    };

    let identity = match (&config.certificate, &config.key) {
        (Some(cert_path), Some(key_path)) => Some(ClientIdentity {
            certificate: read_file(base_dir, cert_path)
                .with_context(|| format!("reading client TLS certificate for {target}"))?,
            key: read_file(base_dir, key_path)
                .with_context(|| format!("reading client TLS key for {target}"))?,
        }),
        (None, None) => None,
        _ => {
            bail!(
                "Client TLS for {target} needs both tls.certificate and tls.key, only one is set"
            );
        }
    };

    Ok(Some(ClientTlsSetup { ca, identity }))
}

/// Builds a reqwest HTTP client honoring an optional client TLS setup.
///
/// `timeout` bounds every request end-to-end (the upstream's `options.timeout`):
/// without it a connected-but-silent upstream would hold a routed request
/// open forever, and the router could never fall through to the next candidate.
///
/// `compress` advertises `Accept-Encoding: gzip` and transparently
/// decompresses responses. Off by default in the upstream config because some
/// nodes send corrupted responses when compression is on (legacy
/// `JsonRpcHttpClient` learned this the hard way); pass it explicitly so the
/// reqwest `gzip` feature's implicit default can't decide it.
pub fn reqwest_client(
    tls: Option<&ClientTlsSetup>,
    timeout: std::time::Duration,
    compress: bool,
) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(timeout).gzip(compress);
    if let Some(setup) = tls {
        if let Some(ca) = &setup.ca {
            builder = builder.tls_built_in_root_certs(false).add_root_certificate(
                reqwest::Certificate::from_pem(ca).context("parsing TLS CA")?,
            );
        }
        if let Some(identity) = &setup.identity {
            // reqwest's rustls backend takes the certificate and key as one
            // PEM bundle.
            let pem = [
                identity.certificate.as_slice(),
                b"\n",
                identity.key.as_slice(),
            ]
            .concat();
            builder = builder.identity(
                reqwest::Identity::from_pem(&pem).context("parsing client TLS identity")?,
            );
        }
    }
    builder.build().context("building HTTP client")
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
    use crate::config::tls::{ClientTlsAuth, TlsClientRequirement, TlsServerCert};

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

    #[test]
    fn client_tls_absent_is_none() {
        let result = client_tls("test", None, Path::new(".")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn client_tls_loads_ca_and_identity() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("ca.crt"), "ca").unwrap();
        std::fs::write(dir.path().join("client.crt"), "cert").unwrap();
        std::fs::write(dir.path().join("client.key"), "key").unwrap();
        let config = ClientTlsAuth {
            ca: Some("ca.crt".to_string()),
            certificate: Some("client.crt".to_string()),
            key: Some("client.key".to_string()),
        };
        let setup = client_tls("test", Some(&config), dir.path())
            .unwrap()
            .unwrap();
        assert_eq!(setup.ca.as_deref(), Some(b"ca".as_slice()));
        let identity = setup.identity.unwrap();
        assert_eq!(identity.certificate, b"cert");
        assert_eq!(identity.key, b"key");
    }

    #[test]
    fn client_tls_certificate_without_key_fails() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("client.crt"), "cert").unwrap();
        let config = ClientTlsAuth {
            ca: None,
            certificate: Some("client.crt".to_string()),
            key: None,
        };
        let result = client_tls("test", Some(&config), dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn client_tls_empty_section_means_tls_with_system_roots() {
        let config = ClientTlsAuth {
            ca: None,
            certificate: None,
            key: None,
        };
        let setup = client_tls("test", Some(&config), Path::new("."))
            .unwrap()
            .unwrap();
        assert!(setup.ca.is_none());
        assert!(setup.identity.is_none());
    }

    #[test]
    fn reqwest_client_from_real_pem() {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tls-local");
        let config = ClientTlsAuth {
            ca: Some("ca.myhost.dev.crt".to_string()),
            certificate: Some("127.0.0.1.crt".to_string()),
            key: Some("127.0.0.1.p8.key".to_string()),
        };
        let setup = client_tls("test", Some(&config), &base).unwrap().unwrap();
        reqwest_client(Some(&setup), std::time::Duration::from_secs(60), false).unwrap();
    }

    #[test]
    fn reqwest_client_without_tls() {
        reqwest_client(None, std::time::Duration::from_secs(60), false).unwrap();
    }

    /// Serves one plain-HTTP request and returns its raw request head, so a
    /// test can assert on the headers a client actually sent.
    fn capture_one_request(listener: std::net::TcpListener) -> std::thread::JoinHandle<String> {
        use std::io::{Read, Write};
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}")
                .unwrap();
            String::from_utf8_lossy(&buf[..n]).to_string()
        })
    }

    // The `compress` flag exists because it was once parsed but silently
    // ignored: assert it changes what actually goes on the wire.
    #[tokio::test(flavor = "multi_thread")]
    async fn reqwest_client_compression_on_the_wire() {
        for compress in [true, false] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            let head = capture_one_request(listener);

            let client = reqwest_client(None, std::time::Duration::from_secs(5), compress).unwrap();
            client.get(&url).send().await.unwrap();

            let head = head.join().unwrap().to_lowercase();
            let advertises_gzip = head.contains("accept-encoding") && head.contains("gzip");
            assert_eq!(advertises_gzip, compress, "compress={compress}: {head}");
        }
    }
}
