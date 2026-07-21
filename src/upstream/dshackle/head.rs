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
//! chain. Each `ChainHead` message is upgraded to the full block — fetched
//! back through the remote's `NativeCall` — and pushed through
//! [`update_with_block`](crate::upstream::head::CurrentHead::update_with_block).
//!
//! The subscription auto-reconnects on stream close or error, with a brief
//! pause between retries.

use crate::blockchain::{BlockchainType, TargetBlockchain};
use crate::data::{BlockContainer, BlockId};
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::dshackle::DshackleUpstream;
use crate::upstream::id::UpstreamId;
use crate::upstream::traits::RpcUpstream;
use emerald_api::proto::common::Chain;
use std::sync::Arc;
use std::time::Duration;

/// Pause between reconnection attempts.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Spawns a background task that subscribes to `SubscribeHead` on the remote
/// Dshackle and updates the upstream's shared head tracker with block data.
///
/// Re-subscribes automatically when the stream closes or encounters an error.
pub fn start_head_subscriber(upstream_id: UpstreamId, chain_ref: i32, upstream: Arc<DshackleUpstream>) {
    let blockchain_type = TargetBlockchain::try_from(chain_ref)
        .map(|chain| chain.blockchain_type())
        .unwrap_or(BlockchainType::Unknown);
    tokio::spawn(async move {
        loop {
            subscribe_once(&upstream_id, chain_ref, blockchain_type, &upstream).await;
            tokio::time::sleep(RETRY_DELAY).await;
        }
    });
}

async fn subscribe_once(
    upstream_id: &UpstreamId,
    chain_ref: i32,
    blockchain_type: BlockchainType,
    upstream: &DshackleUpstream,
) {
    let chain = Chain { r#type: chain_ref };
    let mut client = upstream.grpc_client();
    let head = upstream.head_height();

    match client.subscribe_head(chain).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            tracing::debug!(upstream = %upstream_id, chain = chain_ref, "listening for head updates");

            loop {
                match stream.message().await {
                    Ok(Some(chain_head)) => {
                        match parse_chain_head(
                            &chain_head.block_id,
                            chain_head.height,
                            chain_head.timestamp,
                            &chain_head.weight,
                        ) {
                            Some(bare) => {
                                let block = enrich(upstream, blockchain_type, bare).await;
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

/// Fetch the announced head's full block through the remote's `NativeCall`
/// and replace the bare `ChainHead` data with it.
///
/// The announcement itself carries no parent hash — PoS fork verification
/// rejects a parent-less block as unverifiable, which would permanently flag
/// the remote as forked — and no block data. The full block fixes both, and
/// warms the cache with the hottest possible entry: the `CachingHead`
/// following this head stores blocks that carry their JSON, so the block is
/// usually cached before the first client asks for it. Any failure falls
/// back to the bare head — tracking the height must never stall on the
/// extra fetch.
async fn enrich(
    upstream: &DshackleUpstream,
    blockchain_type: BlockchainType,
    bare: BlockContainer,
) -> BlockContainer {
    let request = match blockchain_type {
        BlockchainType::Ethereum => JsonRpcRequest::new(
            0,
            "eth_getBlockByHash".into(),
            serde_json::json!([bare.hash.to_hex_prefixed(), false]),
        ),
        BlockchainType::Bitcoin => JsonRpcRequest::new(
            0,
            "getblock".into(),
            serde_json::json!([bare.hash.to_hex(), 1]),
        ),
        BlockchainType::Unknown => return bare,
    };
    let response = match upstream.call(&request).await {
        Ok(response) if response.error.is_none() => response,
        Ok(_) | Err(_) => {
            tracing::debug!(hash = %bare.hash, "head block fetch failed, keeping the bare head");
            return bare;
        }
    };
    let full = response
        .result
        .and_then(|raw| enriched_block(&bare, blockchain_type, raw.get()));
    match full {
        Some(full) => full,
        None => {
            tracing::debug!(hash = %bare.hash, "head block not parseable, keeping the bare head");
            bare
        }
    }
}

/// The bare head replaced by its parsed full block. Keeps the announced
/// cumulative work when the block JSON doesn't state it (post-merge Ethereum
/// clients omit `totalDifficulty`).
fn enriched_block(
    bare: &BlockContainer,
    blockchain_type: BlockchainType,
    raw_json: &str,
) -> Option<BlockContainer> {
    let mut full = match blockchain_type {
        BlockchainType::Ethereum => crate::upstream::ethereum::head::parse_eth_block(raw_json)?,
        BlockchainType::Bitcoin => crate::upstream::bitcoin::head::parse_btc_block(raw_json)?,
        BlockchainType::Unknown => return None,
    };
    // A response for a different block must not poison the head.
    if full.hash != bare.hash {
        return None;
    }
    if full.total_difficulty.is_zero() {
        full.total_difficulty = bare.total_difficulty;
    }
    Some(full)
}

/// Build a [`BlockContainer`] from the fields available in a `ChainHead`
/// gRPC message. The remote doesn't provide parent hash or transaction
/// hashes, so those fields are empty until [`enrich`] fills them in.
///
/// `weight` carries the cumulative work as a trimmed big-endian integer
/// (empty when the remote doesn't know it) and feeds the merged head's
/// same-height difficulty ordering. Zero when absent or wider than 256 bits —
/// the merge still advances on height alone, only conflict resolution loses
/// the difficulty signal.
fn parse_chain_head(
    block_id: &str,
    height: u64,
    timestamp_ms: u64,
    weight: &[u8],
) -> Option<BlockContainer> {
    let hash: BlockId = block_id.parse().ok()?;
    // ChainHead.timestamp is milliseconds since epoch
    let timestamp = jiff::Timestamp::from_millisecond(timestamp_ms as i64).ok()?;
    let total_difficulty =
        alloy::primitives::U256::try_from_be_slice(weight).unwrap_or(alloy::primitives::U256::ZERO);

    Some(BlockContainer {
        hash,
        height,
        parent_hash: None,
        total_difficulty,
        timestamp,
        transaction_hashes: vec![],
        json: None,
        header_json: None,
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
            &[],
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
        // No weight reported — the difficulty signal is simply absent.
        assert!(block.total_difficulty.is_zero());
    }

    #[test]
    fn parse_chain_head_weight_as_total_difficulty() {
        // The trimmed big-endian encoding the serving side produces:
        // 1024 == 0x0400.
        let block = parse_chain_head(
            "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75",
            1,
            0,
            &[0x04, 0x00],
        )
        .unwrap();
        assert_eq!(block.total_difficulty, alloy::primitives::U256::from(1024));
    }

    #[test]
    fn parse_chain_head_oversized_weight_is_ignored() {
        // Wider than 256 bits cannot be an honest cumulative work value;
        // dropped rather than panicking in `from_be_slice`.
        let block = parse_chain_head(
            "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75",
            1,
            0,
            &[0xff; 33],
        )
        .unwrap();
        assert!(block.total_difficulty.is_zero());
    }

    #[test]
    fn parse_chain_head_invalid_hash() {
        assert!(parse_chain_head("not-a-hash", 100, 0, &[]).is_none());
    }

    const HASH: &str = "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";

    fn bare_head() -> BlockContainer {
        parse_chain_head(HASH, 16, 1_566_423_564_000, &[0x04, 0x00]).unwrap()
    }

    #[test]
    fn enriched_block_fills_parent_hash_and_json() {
        let raw = format!(
            r#"{{"hash":"0x{HASH}","number":"0x10","parentHash":"0x{parent}","timestamp":"0x5d5d4a0c","transactions":[]}}"#,
            parent = "aa".repeat(32),
        );
        let full = enriched_block(&bare_head(), BlockchainType::Ethereum, &raw).unwrap();
        assert_eq!(full.hash, bare_head().hash);
        assert!(full.parent_hash.is_some());
        assert!(full.json.is_some());
        // No totalDifficulty in the block (post-merge): the announced weight
        // stays as the difficulty signal.
        assert_eq!(full.total_difficulty, alloy::primitives::U256::from(1024));
    }

    #[test]
    fn enriched_block_prefers_the_blocks_own_total_difficulty() {
        let raw = format!(
            r#"{{"hash":"0x{HASH}","number":"0x10","parentHash":"0x{parent}","timestamp":"0x5d5d4a0c","totalDifficulty":"0x800","transactions":[]}}"#,
            parent = "aa".repeat(32),
        );
        let full = enriched_block(&bare_head(), BlockchainType::Ethereum, &raw).unwrap();
        assert_eq!(full.total_difficulty, alloy::primitives::U256::from(0x800));
    }

    #[test]
    fn enriched_block_rejects_a_different_block() {
        // A response for another hash must not replace the announced head.
        let raw = format!(
            r#"{{"hash":"0x{other}","number":"0x10","parentHash":"0x{parent}","timestamp":"0x5d5d4a0c","transactions":[]}}"#,
            other = "bb".repeat(32),
            parent = "aa".repeat(32),
        );
        assert!(enriched_block(&bare_head(), BlockchainType::Ethereum, &raw).is_none());
    }

    #[test]
    fn enriched_block_rejects_garbage() {
        assert!(enriched_block(&bare_head(), BlockchainType::Ethereum, "null").is_none());
        assert!(enriched_block(&bare_head(), BlockchainType::Ethereum, "{}").is_none());
    }
}
