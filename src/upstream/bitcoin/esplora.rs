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

//! Client for the HTTP API of an Esplora address index
//! (<https://github.com/Blockstream/esplora/blob/master/API.md>), porting the
//! legacy `EsploraClient`.
//!
//! A Bitcoin node knows the outputs and history only of the addresses in its
//! own wallet. An Esplora instance running next to the node indexes every
//! address, which is what lets Dshackle answer balance requests for addresses
//! (and xpub keys) the node has never been told about.

use crate::config::tls::BasicAuth;
use serde::Deserialize;
use serde::de::{DeserializeOwned, IgnoredAny};
use std::fmt;

/// One unspent output as Esplora reports it.
#[derive(Debug, Deserialize, PartialEq)]
pub struct EsploraUtxo {
    pub txid: String,
    pub vout: u64,
    /// Amount in satoshis.
    pub value: u64,
    pub status: EsploraTxStatus,
}

/// Confirmation status of the transaction that created an output.
#[derive(Debug, Deserialize, PartialEq)]
pub struct EsploraTxStatus {
    /// `false` while the transaction is still in the mempool.
    pub confirmed: bool,
}

/// Why Esplora can't be asked, or gave no usable answer.
#[derive(Debug)]
pub enum EsploraError {
    /// The configured address isn't an `http`/`https` URL.
    InvalidUrl(String),
    /// The request never completed (connection, TLS, timeout).
    Transport(String),
    /// Esplora answered with a non-200 HTTP status.
    Status(u16),
    /// The body isn't the JSON the API documents.
    InvalidResponse(String),
}

impl fmt::Display for EsploraError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EsploraError::InvalidUrl(url) => {
                write!(f, "invalid Esplora URL, expected http(s)://host: {url}")
            }
            EsploraError::Transport(e) => write!(f, "Esplora is unreachable: {e}"),
            EsploraError::Status(code) => write!(f, "Esplora responded with HTTP {code}"),
            EsploraError::InvalidResponse(e) => write!(f, "invalid Esplora response: {e}"),
        }
    }
}

impl std::error::Error for EsploraError {}

/// An Esplora instance reached over HTTP.
pub struct EsploraClient {
    /// API root, without a trailing slash.
    url: String,
    basic_auth: Option<BasicAuth>,
    client: reqwest::Client,
}

impl EsploraClient {
    /// Creates a client for the API rooted at `url`.
    ///
    /// `basic_auth`, when set, is sent with every request. `client` carries
    /// the transport-level options decided at wiring time (timeout, custom CA,
    /// mutual TLS).
    ///
    /// A `url` no request could be sent to is rejected here, at startup:
    /// Esplora outranks the other balance sources, so a mistyped address would
    /// otherwise fail every balance request of the chain at runtime.
    pub fn new(
        url: &str,
        basic_auth: Option<BasicAuth>,
        client: reqwest::Client,
    ) -> Result<Self, EsploraError> {
        // `host:port` without a scheme parses as a URL of scheme `host`, so
        // parsing alone doesn't catch the most likely mistake.
        let usable = reqwest::Url::parse(url)
            .is_ok_and(|parsed| matches!(parsed.scheme(), "http" | "https") && parsed.has_host());
        if !usable {
            return Err(EsploraError::InvalidUrl(url.to_string()));
        }
        Ok(Self {
            // Request paths start with a slash, so a configured trailing one
            // would produce `//address/..`, which Esplora doesn't route.
            url: url.trim_end_matches('/').to_string(),
            basic_auth,
            client,
        })
    }

    /// Unspent outputs of `address`, including the ones still in the mempool
    /// (`GET /address/:address/utxo`).
    pub async fn utxo(&self, address: &str) -> Result<Vec<EsploraUtxo>, EsploraError> {
        self.get(&format!("/address/{address}/utxo")).await
    }

    /// Whether `address` appears in any transaction, confirmed or in the
    /// mempool (`GET /address/:address/txs`).
    pub async fn has_transactions(&self, address: &str) -> Result<bool, EsploraError> {
        let transactions: Vec<IgnoredAny> = self.get(&format!("/address/{address}/txs")).await?;
        Ok(!transactions.is_empty())
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, EsploraError> {
        let mut request = self.client.get(format!("{}{path}", self.url));
        if let Some(auth) = &self.basic_auth {
            request = request.basic_auth(&auth.username, Some(&auth.password));
        }
        let response = request
            .send()
            .await
            .map_err(|e| EsploraError::Transport(e.to_string()))?;
        let status = response.status();
        if status != reqwest::StatusCode::OK {
            return Err(EsploraError::Status(status.as_u16()));
        }
        let body = response
            .bytes()
            .await
            .map_err(|e| EsploraError::Transport(e.to_string()))?;
        serde_json::from_slice(&body).map_err(|e| EsploraError::InvalidResponse(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::bitcoin::esplora_stub::StubEsplora;
    use std::time::Duration;

    fn client(url: &str, basic_auth: Option<BasicAuth>) -> EsploraClient {
        let http = crate::tls::reqwest_client(None, Duration::from_secs(5), false).unwrap();
        EsploraClient::new(url, basic_auth, http).unwrap()
    }

    fn fixture(name: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata/bitcoin")
            .join(name);
        std::fs::read_to_string(path).unwrap()
    }

    // The legacy `EsploraClientSpec` cases, on the same recorded responses.

    #[tokio::test]
    async fn reads_utxo() {
        let address = "35vktkPo4wdK8Twu4VMiuPLdCx23XEykGY";
        let esplora = StubEsplora::start().await;
        esplora.respond(
            &format!("/address/{address}/utxo"),
            &fixture("esplora-utxo-1.json"),
        );

        let utxo = client(esplora.url(), None).utxo(address).await.unwrap();

        assert_eq!(utxo.len(), 14);
        assert_eq!(
            utxo[0],
            EsploraUtxo {
                txid: "002eba7d9e8081afc687e1c3fa7b6a6451713b75bc91432d441bb1e7e1511c5c".into(),
                vout: 0,
                value: 105_524,
                status: EsploraTxStatus { confirmed: true },
            }
        );
        assert_eq!(
            utxo[12].txid,
            "311d0d1d4eea6ee37d57954cd1002898bef64885d8e438afbbd7fe4fdc6e08de"
        );
        assert_eq!(utxo[12].vout, 2250);
        assert_eq!(utxo.iter().map(|u| u.value).sum::<u64>(), 309_841);
    }

    #[tokio::test]
    async fn address_with_history_has_transactions() {
        let address = "tb1qyatuwvkfx8thy2ntmtuea6v42vp3zefqvll8kx";
        let esplora = StubEsplora::start().await;
        esplora.respond(
            &format!("/address/{address}/txs"),
            &fixture("esplora-txs-1.json"),
        );

        let client = client(esplora.url(), None);
        assert!(client.has_transactions(address).await.unwrap());
    }

    #[tokio::test]
    async fn address_without_history_has_no_transactions() {
        let address = "tb1qyatuwvkfx8thy2ntmtuea6v42vp3zefqvll8kx";
        let esplora = StubEsplora::start().await;
        esplora.respond(&format!("/address/{address}/txs"), "[]");

        let client = client(esplora.url(), None);
        assert!(!client.has_transactions(address).await.unwrap());
    }

    #[tokio::test]
    async fn mempool_output_is_reported_unconfirmed() {
        let esplora = StubEsplora::start().await;
        esplora.respond(
            "/address/addr/utxo",
            r#"[{"txid":"aa","vout":1,"status":{"confirmed":false},"value":700}]"#,
        );

        let utxo = client(esplora.url(), None).utxo("addr").await.unwrap();
        assert!(!utxo[0].status.confirmed);
    }

    #[tokio::test]
    async fn non_200_is_an_error() {
        let esplora = StubEsplora::start().await;
        esplora.fail("/address/addr/utxo", 503);

        let err = client(esplora.url(), None).utxo("addr").await.unwrap_err();
        assert!(matches!(err, EsploraError::Status(503)), "{err}");
    }

    #[tokio::test]
    async fn unexpected_body_is_an_error() {
        let esplora = StubEsplora::start().await;
        esplora.respond("/address/addr/utxo", r#"{"error":"nope"}"#);

        let err = client(esplora.url(), None).utxo("addr").await.unwrap_err();
        assert!(matches!(err, EsploraError::InvalidResponse(_)), "{err}");
    }

    #[tokio::test]
    async fn unreachable_server_is_a_transport_error() {
        // Bind and drop to get a local port nothing listens on.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let err = client(&url, None).utxo("addr").await.unwrap_err();
        assert!(matches!(err, EsploraError::Transport(_)), "{err}");
    }

    #[test]
    fn url_must_be_http_with_a_host() {
        let build = |url: &str| {
            let http = crate::tls::reqwest_client(None, Duration::from_secs(5), false).unwrap();
            EsploraClient::new(url, None, http)
        };

        assert!(build("http://127.0.0.1:3001").is_ok());
        assert!(build("https://esplora.example.com/api/").is_ok());
        for url in [
            "127.0.0.1:3001",
            "localhost:3001",
            "tcp://localhost:3001",
            "",
        ] {
            let Err(err) = build(url) else {
                panic!("accepted {url:?}");
            };
            assert!(matches!(err, EsploraError::InvalidUrl(u) if u == url));
        }
    }

    #[tokio::test]
    async fn sends_basic_auth_and_tolerates_trailing_slash() {
        let esplora = StubEsplora::start().await;
        let auth = BasicAuth {
            username: "user".into(),
            password: "pass".into(),
        };

        let client = client(&format!("{}/", esplora.url()), Some(auth));
        client.utxo("addr").await.unwrap();

        let requests = esplora.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/address/addr/utxo");
        // base64("user:pass")
        assert_eq!(
            requests[0].authorization.as_deref(),
            Some("Basic dXNlcjpwYXNz")
        );
    }
}
