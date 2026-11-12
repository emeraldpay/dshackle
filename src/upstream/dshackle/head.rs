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
use crate::upstream::pause::PauseHandle;
use crate::upstream::traits::RpcUpstream;
use emerald_api::proto::blockchain::ChainHead;
use emerald_api::proto::common::Chain;
use futures::FutureExt;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Status, Streaming};

/// Pause between reconnection attempts.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Limit for one full-block fetch. The head stream waits on it, and a block
/// that slow to fetch isn't worth holding the head back for: the bare head is
/// used instead.
const ENRICH_TIMEOUT: Duration = Duration::from_millis(250);

/// Pauses before each full-block fetch: the first goes out right away, the
/// rest are retries. The remote announces a head as soon as its fastest node
/// has it, so a fetch can land on one of its nodes that hasn't imported the
/// block yet, and a moment later another try usually finds it.
const ENRICH_ATTEMPT_DELAYS: [Duration; 3] = [
    Duration::ZERO,
    Duration::from_millis(50),
    Duration::from_millis(50),
];

/// Spawns a background task that subscribes to `SubscribeHead` on the remote
/// Dshackle and updates the upstream's shared head tracker with block data.
///
/// Re-subscribes automatically when the stream closes or encounters an error.
pub fn start_head_subscriber(
    upstream_id: UpstreamId,
    chain_ref: i32,
    upstream: Arc<DshackleUpstream>,
) {
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
    // A refused subscription takes the upstream out of rotation too, so it
    // shows up even when no call is made to it meanwhile.
    let pause = PauseHandle::new(upstream_id.clone(), Arc::clone(upstream.state()));

    match client.subscribe_head(chain).await {
        Ok(response) => {
            tracing::debug!(upstream = %upstream_id, chain = chain_ref, "listening for head updates");

            let mut announcements = Announcements::new(response.into_inner());
            loop {
                let chain_head = match announcements.next().await {
                    Ok(Some(chain_head)) => chain_head,
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
                        pause.pause_on_refusal(&e.into());
                        break;
                    }
                };
                let Some(bare) = parse_chain_head(
                    &chain_head.block_id,
                    chain_head.height,
                    chain_head.timestamp,
                    &chain_head.weight,
                ) else {
                    tracing::debug!(
                        upstream = %upstream_id,
                        height = chain_head.height,
                        "partial head update (block_id parse failed)"
                    );
                    head.update(chain_head.height);
                    continue;
                };
                let block = enrich(upstream, blockchain_type, bare).await;
                tracing::trace!(
                    upstream = %upstream_id,
                    height = block.height,
                    hash = %block.hash,
                    "head updated via gRPC"
                );
                head.update_with_block(block);
            }
        }
        Err(e) => {
            tracing::debug!(
                upstream = %upstream_id,
                error = %e,
                "subscribe_head failed, retrying"
            );
            pause.pause_on_refusal(&e.into());
        }
    }
}

/// The remote's head announcements, read newest-first: whatever piled up
/// while the previous head was being fetched is skipped down to the latest.
/// Only the current head matters, and on fast chains handling every one in
/// turn would leave the subscriber further and further behind.
struct Announcements {
    stream: Streaming<ChainHead>,
    /// The stream's close or error, met while skipping ahead. Held back so
    /// the head read just before it is still handled; the stream reports an
    /// error only once.
    end: Option<Result<Option<ChainHead>, Status>>,
}

impl Announcements {
    fn new(stream: Streaming<ChainHead>) -> Self {
        Self { stream, end: None }
    }

    /// The newest announcement, waiting for one when none is ready.
    /// `Ok(None)` or an error once the stream is over.
    async fn next(&mut self) -> Result<Option<ChainHead>, Status> {
        if let Some(end) = self.end.take() {
            return end;
        }
        let mut latest = self.stream.message().await;
        while let Ok(Some(_)) = latest {
            // `message()` keeps its progress in the stream, so a poll that
            // finds nothing ready loses nothing.
            match self.stream.message().now_or_never() {
                None => break,
                Some(Ok(Some(newer))) => latest = Ok(Some(newer)),
                Some(end) => {
                    self.end = Some(end);
                    break;
                }
            }
        }
        latest
    }
}

/// Fetch the announced head's full block through the remote's `NativeCall`
/// and replace the bare `ChainHead` data with it.
///
/// The announcement itself carries no parent hash, which PoS fork
/// verification needs to link the block into a chain, and no block data. The
/// full block fixes both, and warms the cache with the hottest possible
/// entry: the `CachingHead` following this head stores blocks that carry
/// their JSON, so the block is usually cached before the first client asks
/// for it. When every attempt fails it falls back to the bare head, since
/// tracking the height must never stall on the extra fetch.
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
    for delay in ENRICH_ATTEMPT_DELAYS {
        tokio::time::sleep(delay).await;
        if let Some(full) = fetch_full(upstream, blockchain_type, &bare, &request).await {
            return full;
        }
    }
    tracing::debug!(upstream = %upstream.id(), hash = %bare.hash, "head block unavailable, keeping the bare head");
    bare
}

/// One attempt at fetching the full block for `bare`.
async fn fetch_full(
    upstream: &DshackleUpstream,
    blockchain_type: BlockchainType,
    bare: &BlockContainer,
    request: &JsonRpcRequest,
) -> Option<BlockContainer> {
    let response = match tokio::time::timeout(ENRICH_TIMEOUT, upstream.call(request)).await {
        Ok(Ok(response)) if response.error.is_none() => response,
        Ok(Ok(response)) => {
            tracing::debug!(upstream = %upstream.id(), hash = %bare.hash, error = ?response.error, "head block fetch failed");
            return None;
        }
        Ok(Err(e)) => {
            tracing::debug!(upstream = %upstream.id(), hash = %bare.hash, error = %e, "head block fetch failed");
            return None;
        }
        Err(_) => {
            tracing::debug!(upstream = %upstream.id(), hash = %bare.hash, "head block fetch timed out");
            return None;
        }
    };
    let full = response
        .result
        .and_then(|raw| enriched_block(bare, blockchain_type, raw.get()));
    if full.is_none() {
        // Usually `null`: the remote routed the call to a node without the block yet.
        tracing::debug!(upstream = %upstream.id(), hash = %bare.hash, "head block not parseable");
    }
    full
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

    const HASH: &str = "fc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75";

    /// Reading the remote's announcement stream.
    mod announcements {
        use super::*;

        /// Decodes test frames the way the generated gRPC client does.
        struct ChainHeadDecoder;

        impl tonic::codec::Decoder for ChainHeadDecoder {
            type Item = ChainHead;
            type Error = Status;

            fn decode(
                &mut self,
                src: &mut tonic::codec::DecodeBuf<'_>,
            ) -> Result<Option<ChainHead>, Status> {
                prost::Message::decode(src)
                    .map(Some)
                    .map_err(|e| Status::internal(e.to_string()))
            }
        }

        /// A stream whose announcements of `heights` have all arrived
        /// already, followed by its close.
        fn arrived(heights: &[u64]) -> Announcements {
            let mut body = Vec::new();
            for &height in heights {
                let message = prost::Message::encode_to_vec(&ChainHead {
                    height,
                    ..Default::default()
                });
                body.push(0); // uncompressed
                body.extend_from_slice(&(message.len() as u32).to_be_bytes());
                body.extend_from_slice(&message);
            }
            let body = http_body_util::Full::new(bytes::Bytes::from(body));
            Announcements::new(Streaming::new_request(ChainHeadDecoder, body, None, None))
        }

        #[tokio::test]
        async fn skip_to_the_newest_waiting() {
            let mut announcements = arrived(&[10, 11, 12]);
            assert_eq!(announcements.next().await.unwrap().unwrap().height, 12);
        }

        #[tokio::test]
        async fn report_the_close_only_after_the_last_head() {
            // The close is met while skipping ahead; it must not swallow the
            // head read before it.
            let mut announcements = arrived(&[10, 11]);
            assert_eq!(announcements.next().await.unwrap().unwrap().height, 11);
            assert!(announcements.next().await.unwrap().is_none());
        }

        #[tokio::test]
        async fn report_the_close_of_an_empty_stream() {
            let mut announcements = arrived(&[]);
            assert!(announcements.next().await.unwrap().is_none());
        }
    }

    /// Turning a `ChainHead` announcement into a bare block.
    mod chain_head {
        use super::*;

        #[test]
        fn becomes_a_block_without_parent_or_data() {
            let block = parse_chain_head(HASH, 8_396_159, 1_566_423_564_000, &[]).unwrap();

            assert_eq!(block.height, 8_396_159);
            assert_eq!(block.hash.to_hex(), HASH);
            assert_eq!(
                block.timestamp,
                jiff::Timestamp::from_millisecond(1_566_423_564_000).unwrap()
            );
            assert!(block.parent_hash.is_none());
            assert!(block.transaction_hashes.is_empty());
            assert!(block.json.is_none());
            // No weight reported: the difficulty signal is simply absent.
            assert!(block.total_difficulty.is_zero());
        }

        #[test]
        fn weight_is_the_total_difficulty() {
            // The trimmed big-endian encoding the serving side produces.
            let block = parse_chain_head(HASH, 1, 0, &[0x04, 0x00]).unwrap();
            assert_eq!(
                block.total_difficulty,
                alloy::primitives::U256::from(0x0400)
            );
        }

        #[test]
        fn oversized_weight_is_ignored() {
            // Wider than 256 bits can't be an honest cumulative work value;
            // dropped rather than panicking in `from_be_slice`.
            let block = parse_chain_head(HASH, 1, 0, &[0xff; 33]).unwrap();
            assert!(block.total_difficulty.is_zero());
        }

        #[test]
        fn invalid_hash_is_not_a_block() {
            assert!(parse_chain_head("not-a-hash", 100, 0, &[]).is_none());
        }
    }

    /// Replacing the bare block with the full one fetched from the remote.
    mod full_block {
        use super::*;

        const PARENT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        /// Announced with a weight of 1024.
        fn bare_head() -> BlockContainer {
            parse_chain_head(HASH, 16, 1_566_423_564_000, &[0x04, 0x00]).unwrap()
        }

        /// `eth_getBlockByHash` result for block `hash`, with `extra` JSON
        /// fields appended.
        fn eth_block(hash: &str, extra: &str) -> String {
            format!(
                r#"{{"hash":"0x{hash}","number":"0x10","parentHash":"0x{PARENT}","timestamp":"0x5d5d4a0c","transactions":[]{extra}}}"#
            )
        }

        fn enrich_eth(raw: &str) -> Option<BlockContainer> {
            enriched_block(&bare_head(), BlockchainType::Ethereum, raw)
        }

        #[test]
        fn fills_in_the_parent_hash_and_block_data() {
            let full = enrich_eth(&eth_block(HASH, "")).unwrap();
            assert_eq!(full.hash, bare_head().hash);
            assert_eq!(full.parent_hash, Some(PARENT.parse().unwrap()));
            assert!(full.json.is_some());
        }

        #[test]
        fn keeps_the_announced_weight_when_the_block_has_none() {
            // Post-merge clients omit totalDifficulty.
            let full = enrich_eth(&eth_block(HASH, "")).unwrap();
            assert_eq!(full.total_difficulty, alloy::primitives::U256::from(1024));
        }

        #[test]
        fn prefers_the_blocks_own_total_difficulty() {
            let full = enrich_eth(&eth_block(HASH, r#","totalDifficulty":"0x800""#)).unwrap();
            assert_eq!(full.total_difficulty, alloy::primitives::U256::from(0x800));
        }

        #[test]
        fn different_block_is_refused() {
            // A response for another hash must not replace the announced head.
            assert!(enrich_eth(&eth_block(&"bb".repeat(32), "")).is_none());
        }

        #[test]
        fn null_is_refused() {
            // What a remote node answers when it hasn't imported the block
            // yet, the case the fetch retries for.
            assert!(enrich_eth("null").is_none());
        }

        #[test]
        fn malformed_block_is_refused() {
            assert!(enrich_eth("{}").is_none());
        }
    }
}
