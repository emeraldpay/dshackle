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
//! chain. Each `ChainHead` message is parsed into a [`BlockContainer`] and
//! pushed through [`CurrentHead::update_with_block`].
//!
//! The subscription auto-reconnects on stream close or error, with a brief
//! pause between retries.

use crate::data::{BlockContainer, BlockId};
use crate::upstream::head::CurrentHead;
use emerald_api::proto::blockchain::blockchain_client::BlockchainClient;
use emerald_api::proto::common::Chain;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

/// Pause between reconnection attempts.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Spawns a background task that subscribes to `SubscribeHead` on the remote
/// Dshackle and updates the shared head tracker with block data.
///
/// Re-subscribes automatically when the stream closes or encounters an error.
pub fn start_head_subscriber(
    upstream_id: String,
    chain_ref: i32,
    client: BlockchainClient<Channel>,
    head: Arc<CurrentHead>,
) {
    tokio::spawn(async move {
        loop {
            subscribe_once(&upstream_id, chain_ref, &client, &head).await;
            tokio::time::sleep(RETRY_DELAY).await;
        }
    });
}

async fn subscribe_once(
    upstream_id: &str,
    chain_ref: i32,
    client: &BlockchainClient<Channel>,
    head: &CurrentHead,
) {
    let chain = Chain { r#type: chain_ref };
    let mut client = client.clone();

    match client.subscribe_head(chain).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            tracing::debug!(upstream = %upstream_id, chain = chain_ref, "listening for head updates");

            loop {
                match stream.message().await {
                    Ok(Some(chain_head)) => {
                        match parse_chain_head(&chain_head.block_id, chain_head.height, chain_head.timestamp) {
                            Some(block) => {
                                tracing::trace!(
                                    upstream = %upstream_id,
                                    height = block.height,
                                    hash = %block.hash,
                                    "head updated via gRPC"
                                );
                                head.update_with_block(block);
                            }
                            None => {
                                // Fallback: use height only
                                tracing::debug!(
                                    upstream = %upstream_id,
                                    height = chain_head.height,
                                    "partial head update (block_id parse failed)"
                                );
                                head.update(chain_head.height);
                            }
                        }
                    }
                    Ok(None) => {
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

/// Build a [`BlockContainer`] from the fields available in a `ChainHead`
/// gRPC message. The remote doesn't provide parent hash or transaction
/// hashes, so those fields are empty.
fn parse_chain_head(
    block_id: &str,
    height: u64,
    timestamp_ms: u64,
) -> Option<BlockContainer> {
    let hash: BlockId = block_id.parse().ok()?;
    // ChainHead.timestamp is milliseconds since epoch
    let timestamp = jiff::Timestamp::from_millisecond(timestamp_ms as i64).ok()?;

    Some(BlockContainer {
        hash,
        height,
        parent_hash: None,
        timestamp,
        transaction_hashes: vec![],
        json: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_chain_head() {
        let block = parse_chain_head(
            "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75",
            8_396_159,
            1_566_423_564_000,
        )
        .unwrap();

        assert_eq!(block.height, 8_396_159);
        assert_eq!(
            block.hash.to_hex(),
            "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75"
        );
        assert!(block.parent_hash.is_none());
        assert!(block.transaction_hashes.is_empty());
        assert!(block.json.is_none());
    }

    #[test]
    fn parse_chain_head_invalid_hash() {
        assert!(parse_chain_head("not-a-hash", 100, 0).is_none());
    }
}
