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

//! Emerald Dshackle — Fault Tolerant Load Balancer for Blockchain API.

mod blockchain;
mod config;
mod global;
mod jsonrpc;
mod rpc;
mod server;
mod upstream;

use clap::Parser;
use shadow_rs::shadow;
use std::path::PathBuf;
use std::sync::Arc;

shadow!(build);

/// Formats the version string to match the legacy output:
/// `0.18.0-dev built from 09d22615 on 2026-04-06T15:48:36 UTC for macos-x86_64`
fn version() -> String {
    let date = jiff::Timestamp::from_second(build::BUILD_TIMESTAMP)
        .expect("valid build timestamp")
        .strftime("%Y-%m-%dT%H:%M:%S UTC");
    format!(
        "{} built from {} on {} for {}",
        build::PKG_VERSION,
        build::SHORT_COMMIT,
        date,
        build::BUILD_OS,
    )
}

const BANNER: &str = r#"
                               _     _     __ _     _                _    _
                              | |   | |   / /| |   | |              | |  | |
  ___ _ __ ___   ___ _ __ __ _| | __| |  / /_| |___| |__   __ _  ___| | _| | ___
 / _ \ '_ ` _ \ / _ \ '__/ _` | |/ _` | / / _` / __| '_ \ / _` |/ __| |/ / |/ _ \
|  __/ | | | | |  __/ | | (_| | | (_| |/ / (_| \__ \ | | | (_| | (__|   <| |  __/
 \___|_| |_| |_|\___|_|  \__,_|_|\__,_/_/ \__,_|___/_| |_|\__,_|\___|_|\_\_|\___|
  Emerald Dshackle - Fault Tolerant Load Balancer for Blockchain API
  https://github.com/emeraldpay/dshackle"#;

/// Emerald Dshackle - Fault Tolerant Load Balancer for Blockchain API
#[derive(Parser, Debug)]
#[command(name = "dshackle", version = version())]
struct Cli {
    /// Path to the configuration file
    #[arg(long = "configPath")]
    config_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    // Multiple deps enable both `ring` and `aws-lc-rs` features on rustls,
    // so no automatic default is chosen. Install one explicitly before any
    // TLS connection is attempted.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install default TLS crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    println!("{}", BANNER);
    println!("  v{}\n", version());

    let config_path = match config::resolve_config_path(cli.config_path.as_deref()) {
        Ok(path) => path,
        Err(e) => {
            tracing::error!("{}", e);
            std::process::exit(1);
        }
    };

    tracing::info!("Using config: {}", config_path.display());

    let main_config = match config::read_config(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            tracing::error!("Failed to read config: {e:#}");
            std::process::exit(1);
        }
    };

    global::set_config(main_config).expect("CONFIG already initialized");
    tracing::info!("Configuration loaded successfully");

    let config = global::CONFIG.get().expect("CONFIG must be initialized");

    // Build upstreams from configuration
    let upstreams_config = match &config.upstreams {
        Some(cfg) => cfg,
        None => {
            tracing::error!("No upstreams configured");
            std::process::exit(1);
        }
    };

    let upstreams = match upstream::UpstreamManager::from_config(upstreams_config).await {
        Ok(mgr) => Arc::new(mgr),
        Err(e) => {
            tracing::error!("Failed to build upstreams: {e:#}");
            std::process::exit(1);
        }
    };

    // Start gRPC server
    let service = rpc::blockchain_rpc::BlockchainRpcService::new(upstreams);

    if let Err(e) = server::start_grpc_server(&config.host, config.port, service).await {
        tracing::error!("gRPC server failed: {e:#}");
        std::process::exit(1);
    }
}
