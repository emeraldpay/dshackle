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

//! gRPC server setup and startup.

use crate::rpc::blockchain_rpc::BlockchainRpcService;
use crate::tls::{ClientAuth, ServerTlsSetup};
use emerald_api::proto::blockchain::blockchain_server::BlockchainServer;
use std::net::SocketAddr;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::ServerTlsConfig;
use tonic::transport::{Certificate, Identity};

impl From<ServerTlsSetup> for ServerTlsConfig {
    fn from(setup: ServerTlsSetup) -> Self {
        let tls = ServerTlsConfig::new().identity(Identity::from_pem(setup.certificate, setup.key));
        match setup.client {
            ClientAuth::TrustAll => tls,
            ClientAuth::Optional(ca) => tls
                .client_ca_root(Certificate::from_pem(ca))
                .client_auth_optional(true),
            ClientAuth::Required(ca) => tls
                .client_ca_root(Certificate::from_pem(ca))
                .client_auth_optional(false),
        }
    }
}

/// Start the gRPC server listening on the given host and port.
///
/// With a TLS setup the server accepts only TLS connections; without one it
/// serves plaintext.
///
/// With `compress` the server accepts gzip-encoded requests and gzips
/// responses to clients that advertise support. Legacy registers both the
/// compressor and decompressor registries, so it goes both directions here
/// too; gRPC negotiation keeps it compatible with clients that don't compress.
pub async fn start_grpc_server(
    host: &str,
    port: u16,
    tls: Option<ServerTlsSetup>,
    compress: bool,
    service: BlockchainRpcService,
) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("{host}:{port}").parse()?;

    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls.into())?;
    }

    let mut service = BlockchainServer::new(service);
    if compress {
        service = service
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip);
    }

    tracing::info!("gRPC server listening on {}", addr);

    builder.add_service(service).serve(addr).await?;

    Ok(())
}
