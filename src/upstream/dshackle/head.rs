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

//! Head tracking for Dshackle gRPC upstreams via `SubscribeHead`.
//!
//! Subscribes to the remote Dshackle's `SubscribeHead` stream for a specific
//! chain. Each `ChainHead` message contains the block height which is used
//! to update the shared `CurrentHeight`.
//!
//! The subscription auto-reconnects on stream close or error, with a brief
//! pause between retries.

use crate::upstream::head::CurrentHeight;
use emerald_api::proto::blockchain::blockchain_client::BlockchainClient;
use emerald_api::proto::common::Chain;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

/// Pause between reconnection attempts.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Spawns a background task that subscribes to `SubscribeHead` on the remote
/// Dshackle and updates the shared height tracker.
///
/// Re-subscribes automatically when the stream closes or encounters an error.
pub fn start_head_subscriber(
    upstream_id: String,
    chain_ref: i32,
    client: BlockchainClient<Channel>,
    height: Arc<CurrentHeight>,
) {
    tokio::spawn(async move {
        loop {
            subscribe_once(&upstream_id, chain_ref, &client, &height).await;
            tokio::time::sleep(RETRY_DELAY).await;
        }
    });
}

async fn subscribe_once(
    upstream_id: &str,
    chain_ref: i32,
    client: &BlockchainClient<Channel>,
    height: &CurrentHeight,
) {
    let chain = Chain { r#type: chain_ref };
    let mut client = client.clone();

    match client.subscribe_head(chain).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            tracing::debug!(upstream = %upstream_id, chain = chain_ref, "listening for head updates");

            loop {
                match stream.message().await {
                    Ok(Some(head)) => {
                        tracing::trace!(
                            upstream = %upstream_id,
                            height = head.height,
                            block = %head.block_id,
                            "head updated via gRPC"
                        );
                        height.update(head.height);
                    }
                    Ok(None) => {
                        // Stream ended gracefully
                        tracing::debug!(
                            upstream = %upstream_id,
                            "head stream closed, will re-subscribe"
                        );
                        break;
                    }
                    Err(e) => {
                        tracing::debug!(
                            upstream = %upstream_id,
                            error = %e,
                            "head stream error, will re-subscribe"
                        );
                        break;
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!(
                upstream = %upstream_id,
                error = %e,
                "subscribe_head failed, retrying"
            );
        }
    }
}
