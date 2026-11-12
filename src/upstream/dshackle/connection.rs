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

//! The gRPC connection to a remote Dshackle.

use crate::config::upstreams::DshackleConnection;
use crate::tls::ClientTlsSetup;
use crate::upstream::USER_AGENT;
use anyhow::Context as _;
use emerald_api::conn::EmeraldConn;
use emerald_api::creds::{AuthService, Credentials};
use emerald_api::proto::blockchain::blockchain_client::BlockchainClient;
use emerald_api::proto::blockchain::{DescribeChain, DescribeRequest};
use tonic::codegen::http::uri::{PathAndQuery, Scheme};
use tonic::transport::{Channel, Uri};

/// Port of a remote Dshackle configured by `host` alone.
const DEFAULT_PORT: u16 = 2448;

/// A client of a remote Dshackle.
///
/// Every call made through it, and through any of its clones, carries the
/// credentials of the connection it was made for. With no credentials
/// configured, the calls go as they are.
pub type RemoteClient = BlockchainClient<AuthService<Channel>>;

/// A client of a remote Dshackle over `channel`, authenticated with `credentials`.
pub fn remote_client(channel: Channel, credentials: Credentials) -> RemoteClient {
    emerald_api::blockchain::connect(&EmeraldConn::new(channel, credentials))
}

/// Where and how to connect to a remote Dshackle.
pub struct RemoteEndpoint {
    /// Its scheme is the one place that decides whether the connection is
    /// encrypted: tonic applies TLS only to an `https` URI.
    uri: Uri,
    tls: Option<ClientTlsSetup>,
    compress: bool,
    credentials: Credentials,
}

impl RemoteEndpoint {
    /// The endpoint of a configured dshackle connection, or `None` when it has
    /// neither `url` nor `host`.
    pub fn new(
        config: &DshackleConnection,
        tls: Option<ClientTlsSetup>,
    ) -> anyhow::Result<Option<Self>> {
        let Some(uri) = resolve_uri(config, tls.is_some())? else {
            return Ok(None);
        };
        Ok(Some(Self {
            uri,
            tls,
            // Unlike HTTP/WS upstreams this defaults to on, as in legacy: gRPC
            // negotiates the encoding per call, so a remote without gzip
            // support is not harmed.
            compress: config.compress.unwrap_or(true),
            credentials: config
                .emerald_auth
                .as_ref()
                .map(Credentials::from)
                .unwrap_or_default(),
        }))
    }

    pub fn uri(&self) -> &Uri {
        &self.uri
    }

    fn is_encrypted(&self) -> bool {
        self.uri.scheme() == Some(&Scheme::HTTPS)
    }

    /// Whether the auth token would travel in cleartext.
    pub fn exposes_credentials(&self) -> bool {
        !self.is_encrypted() && matches!(self.credentials, Credentials::Token(_))
    }

    /// Connects to the remote and calls `Describe` to discover its available
    /// chains. With credentials, this first call is also the one that
    /// authenticates, so a rejected token fails it.
    pub async fn connect_and_describe(&self) -> anyhow::Result<(RemoteClient, Vec<DescribeChain>)> {
        // tonic always appends its own `tonic/<ver>` token, so the remote sees
        // `EmeraldDshackle/<ver> tonic/<ver>`.
        let mut endpoint =
            tonic::transport::Endpoint::from(self.uri.clone()).user_agent(USER_AGENT)?;
        // An `https` URL enables TLS by itself, even with no `tls` section on the
        // upstream: tonic refuses to connect to an https endpoint that has no TLS
        // configured, while every other client here (reqwest, tungstenite) takes
        // TLS from the scheme. Without this an `https` gRPC upstream would fail
        // with nothing but "transport error".
        if self.is_encrypted() {
            let tls = self.tls.as_ref();
            let tls_config = match tls.and_then(|setup| setup.ca.as_ref()) {
                Some(ca) => tonic::transport::ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(ca)),
                None => tonic::transport::ClientTlsConfig::new().with_native_roots(),
            };
            let tls_config = match tls.and_then(|setup| setup.identity.as_ref()) {
                Some(identity) => tls_config.identity(tonic::transport::Identity::from_pem(
                    &identity.certificate,
                    &identity.key,
                )),
                None => tls_config,
            };
            endpoint = endpoint.tls_config(tls_config)?;
        }
        let channel = endpoint
            .connect()
            .await
            .with_context(|| format!("connecting to {}", self.uri))?;
        let mut client = remote_client(channel, self.credentials.clone());
        if self.compress {
            // Accept-only, like legacy: its channel registries let the client
            // advertise and decode gzip, but requests were never compressed
            // (that needed a per-call compressor legacy didn't set). Request
            // compression is also not negotiated in gRPC — a remote without gzip
            // would reject such calls outright, so don't send compressed.
            client = client.accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        }

        let describe_resp = client
            .describe(DescribeRequest {})
            .await
            .context("calling Describe")?;
        Ok((client, describe_resp.into_inner().chains))
    }
}

/// The URI of a dshackle connection, with the scheme matching whether TLS is
/// configured: a `tls` section makes even an `http` URL encrypted, and a URL
/// without a scheme (legacy accepted `host:port`) gets one.
fn resolve_uri(config: &DshackleConnection, tls: bool) -> anyhow::Result<Option<Uri>> {
    let uri: Uri = match (&config.url, &config.host) {
        (Some(url), _) => url
            .parse()
            .with_context(|| format!("invalid dshackle url `{url}`"))?,
        (None, Some(host)) => format!("{host}:{}", config.port.unwrap_or(DEFAULT_PORT))
            .parse()
            .with_context(|| format!("invalid dshackle host `{host}`"))?,
        (None, None) => return Ok(None),
    };
    let mut parts = uri.into_parts();
    if tls {
        parts.scheme = Some(Scheme::HTTPS);
    } else if parts.scheme.is_none() {
        parts.scheme = Some(Scheme::HTTP);
    }
    if parts.path_and_query.is_none() {
        parts.path_and_query = Some(PathAndQuery::from_static("/"));
    }
    Uri::from_parts(parts)
        .map(Some)
        .with_context(|| "invalid dshackle url".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::dshackle::stub_remote::{JWT, StubRemote, TOKEN};
    use emerald_api::proto::blockchain::{NativeCallItem, NativeCallRequest};
    use emerald_api::proto::common;
    use std::time::Duration;
    use tokio_stream::StreamExt;
    use tonic::{Code, Status};

    fn endpoint(url: String, credentials: Credentials) -> RemoteEndpoint {
        RemoteEndpoint {
            uri: url.parse().unwrap(),
            tls: None,
            compress: false,
            credentials,
        }
    }

    // ── Address ──────────────────────────────────────────────────────────

    fn config(yaml: &str) -> DshackleConnection {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn uri_of(yaml: &str, tls: bool) -> String {
        resolve_uri(&config(yaml), tls)
            .unwrap()
            .unwrap()
            .to_string()
    }

    #[test]
    fn url_is_used_as_is() {
        assert_eq!(
            uri_of("url: http://remote:2449", false),
            "http://remote:2449/"
        );
        assert_eq!(uri_of("url: https://remote", false), "https://remote/");
    }

    #[test]
    fn tls_section_makes_url_encrypted() {
        assert_eq!(
            uri_of("url: http://remote:2449", true),
            "https://remote:2449/"
        );
        assert_eq!(
            uri_of("url: HTTP://remote:2449", true),
            "https://remote:2449/"
        );
    }

    #[test]
    fn scheme_is_case_insensitive() {
        assert_eq!(uri_of("url: HTTPS://remote", false), "https://remote/");
        assert_eq!(uri_of("url: HTTP://remote", false), "http://remote/");
    }

    #[test]
    fn url_without_scheme_gets_one() {
        assert_eq!(uri_of("url: remote:2449", false), "http://remote:2449/");
        assert_eq!(uri_of("url: remote:2449", true), "https://remote:2449/");
    }

    #[test]
    fn host_with_default_port() {
        assert_eq!(uri_of("host: remote", false), "http://remote:2448/");
        assert_eq!(
            uri_of("{host: remote, port: 2449}", true),
            "https://remote:2449/"
        );
    }

    #[test]
    fn no_address_is_none() {
        assert!(
            resolve_uri(&config("compress: true"), false)
                .unwrap()
                .is_none()
        );
    }

    // ── TLS negotiation ──────────────────────────────────────────────────

    /// Serves one TCP connection and returns the first bytes the client sent,
    /// which tell a TLS ClientHello (0x16) from a cleartext HTTP/2 preface.
    fn capture_first_bytes(listener: std::net::TcpListener) -> std::thread::JoinHandle<Vec<u8>> {
        use std::io::Read;
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 16];
            let n = stream.read(&mut buf).unwrap_or(0);
            buf[..n].to_vec()
        })
    }

    /// Nothing answers the socket, so the connection never completes; the
    /// assertion is about what the client put on the wire before giving up.
    async fn first_bytes_sent_to(url_scheme: &str) -> Vec<u8> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("{url_scheme}://{}", listener.local_addr().unwrap());
        let captured = capture_first_bytes(listener);

        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            endpoint(url, Credentials::None).connect_and_describe(),
        )
        .await;

        captured.join().unwrap()
    }

    // An `https` gRPC upstream with no `tls` section used to fail before
    // reaching the network, with tonic's "Connecting to HTTPS without TLS
    // enabled" buried under a bare "transport error".
    #[tokio::test(flavor = "multi_thread")]
    async fn https_url_negotiates_tls_without_a_tls_section() {
        let bytes = first_bytes_sent_to("https").await;
        assert_eq!(
            bytes.first(),
            Some(&0x16),
            "expected a TLS ClientHello, got {bytes:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_url_stays_cleartext() {
        let bytes = first_bytes_sent_to("http").await;
        assert!(
            bytes.starts_with(b"PRI * HTTP/2"),
            "expected a cleartext HTTP/2 preface, got {bytes:?}"
        );
    }

    // ── Token auth ───────────────────────────────────────────────────────

    #[test]
    fn token_over_cleartext_is_exposed() {
        let token = || Credentials::token(TOKEN);
        assert!(endpoint("http://remote:2449".into(), token()).exposes_credentials());
        assert!(!endpoint("https://remote:2449".into(), token()).exposes_credentials());
        assert!(!endpoint("HTTPS://remote:2449".into(), token()).exposes_credentials());
        assert!(!endpoint("http://remote:2449".into(), Credentials::None).exposes_credentials());
    }

    #[tokio::test]
    async fn token_auth_covers_every_kind_of_call() {
        let remote = StubRemote::new();
        let url = remote.start().await;

        let (mut client, chains) = endpoint(url, Credentials::token(TOKEN))
            .connect_and_describe()
            .await
            .unwrap();
        assert_eq!(chains.len(), 1);

        let reply = client
            .clone()
            .native_call(NativeCallRequest {
                chain: common::ChainRef::ChainEthereum as i32,
                items: vec![NativeCallItem {
                    id: 1,
                    method: "eth_blockNumber".into(),
                    payload: b"[]".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert!(reply.succeed);

        let head = client
            .subscribe_head(common::Chain {
                r#type: common::ChainRef::ChainEthereum as i32,
            })
            .await
            .unwrap()
            .into_inner()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.height, 100);

        let bearer = format!("Bearer {JWT}");
        assert_eq!(
            remote.authorization(),
            vec![bearer.clone(), bearer.clone(), bearer]
        );
    }

    #[tokio::test]
    async fn no_token_sends_no_header() {
        let remote = StubRemote::new();
        let url = remote.start().await;

        endpoint(url, Credentials::None)
            .connect_and_describe()
            .await
            .unwrap();

        assert_eq!(remote.authorization(), vec![String::new()]);
    }

    #[tokio::test]
    async fn rejected_token_fails_describe_as_unauthenticated() {
        let remote = StubRemote::new();
        let url = remote.start().await;

        let err = endpoint(url, Credentials::token("wrong-token"))
            .connect_and_describe()
            .await
            .unwrap_err();

        let status = err.downcast_ref::<Status>().expect("a gRPC status");
        assert_eq!(status.code(), Code::Unauthenticated);
        assert!(format!("{err:#}").contains("unknown token"), "{err:#}");
        assert!(
            remote.authorization().is_empty(),
            "must not reach the remote"
        );
    }
}
