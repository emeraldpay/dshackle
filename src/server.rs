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
use emerald_api::proto::blockchain::blockchain_server::BlockchainServer;
use std::net::SocketAddr;

/// Start the gRPC server listening on the given host and port.
pub async fn start_grpc_server(
    host: &str,
    port: u16,
    service: BlockchainRpcService,
) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("{host}:{port}").parse()?;

    tracing::info!("gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(BlockchainServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
