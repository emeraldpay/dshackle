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

mod config;

use clap::Parser;
use std::path::PathBuf;
use tracing::{error, info};

/// Emerald Dshackle - Fault Tolerant Load Balancer for Blockchain API
#[derive(Parser, Debug)]
#[command(name = "dshackle", version)]
struct Cli {
    /// Path to the configuration file
    #[arg(long = "configPath")]
    config_path: Option<PathBuf>,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    let config_path = match config::resolve_config_path(cli.config_path.as_deref()) {
        Ok(path) => path,
        Err(e) => {
            error!("{}", e);
            std::process::exit(1);
        }
    };

    info!("Using config: {}", config_path.display());
}
