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

//! Egress (server-pushed) subscriptions.
//!
//! The topics behind the gRPC `NativeSubscribe` and, later, `eth_subscribe`
//! over the WebSocket proxy. Each chain exposes an [`EgressSubscription`] that
//! turns a topic + params into a stream of JSON messages. Ported from the
//! legacy `EgressSubscription` / `EthereumEgressSubscription`.

use crate::data::BlockContainer;
use crate::upstream::merged_head::MergedHead;
use serde_json::json;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

/// Subscription topic name. The other `eth_subscribe` topics (`logs`,
/// `syncing`, `newPendingTransactions`) are added as they are implemented.
pub const METHOD_NEW_HEADS: &str = "newHeads";

/// A stream of JSON-encoded subscription messages.
pub type SubscriptionStream = Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>;

/// Why a subscription could not be started.
#[derive(Debug)]
pub enum EgressError {
    /// The topic isn't supported on this chain.
    UnsupportedMethod(String),
}

/// Server-pushed subscriptions for one chain.
pub trait EgressSubscription: Send + Sync {
    /// Subscribe to `method` with optional JSON `params`, returning a stream of
    /// JSON messages or an error when the topic isn't supported.
    fn subscribe(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<SubscriptionStream, EgressError>;
}

/// Egress backed solely by the chain's merged head — supports `newHeads` only.
/// The richer Ethereum topics (`logs`, `syncing`) build on top of this as they
/// are ported.
pub struct HeadEgress {
    head: Arc<MergedHead>,
}

impl HeadEgress {
    pub fn new(head: Arc<MergedHead>) -> Self {
        Self { head }
    }
}

impl EgressSubscription for HeadEgress {
    fn subscribe(
        &self,
        method: &str,
        _params: Option<serde_json::Value>,
    ) -> Result<SubscriptionStream, EgressError> {
        match method {
            METHOD_NEW_HEADS => {
                let stream = BroadcastStream::new(self.head.subscribe())
                    // Skip the gap markers the broadcast emits when a slow
                    // subscriber falls behind; only the latest head matters.
                    .filter_map(|item| item.ok().map(|block| new_head_message(&block)));
                Ok(Box::pin(stream))
            }
            other => Err(EgressError::UnsupportedMethod(other.to_string())),
        }
    }
}

/// Build a `newHeads` message from a head block.
///
/// Emits the subset of header fields carried by [`BlockContainer`]. The rest of
/// the legacy `NewHeadMessage` (difficulty, gasLimit, gasUsed, logsBloom,
/// miner, baseFeePerGas) requires the raw header JSON, which the head pipeline
/// will preserve when the WebSocket egress lands; until then those fields are
/// omitted rather than guessed.
fn new_head_message(block: &BlockContainer) -> Vec<u8> {
    let value = json!({
        "number": format!("0x{:x}", block.height),
        "hash": block.hash.to_hex_prefixed(),
        "parentHash": block.parent_hash.map(|p| p.to_hex_prefixed()),
        "timestamp": format!("0x{:x}", block.timestamp.as_second() as u64),
    });
    serde_json::to_vec(&value).expect("newHeads message always serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::BlockId;
    use crate::upstream::head::CurrentHead;

    fn block(height: u64) -> BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = height as u8;
        BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: Some(BlockId::from_bytes([0xaa; 32])),
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::from_second(1_700_000_000).unwrap(),
            transaction_hashes: vec![],
            json: None,
        }
    }

    #[tokio::test]
    async fn new_heads_streams_head_messages() {
        let head = Arc::new(CurrentHead::new());
        let merged = MergedHead::new(vec![Arc::clone(&head)]);
        let egress = HeadEgress::new(merged);

        let mut stream = egress.subscribe(METHOD_NEW_HEADS, None).unwrap();
        head.update_with_block(block(0x10));

        let msg = stream.next().await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&msg).unwrap();
        assert_eq!(value["number"], "0x10");
        assert_eq!(value["timestamp"], "0x6553f100");
        assert!(value["hash"].as_str().unwrap().starts_with("0x"));
        assert!(value["parentHash"].as_str().unwrap().starts_with("0x"));
    }

    #[tokio::test]
    async fn unsupported_method_errors() {
        let merged = MergedHead::new(vec![]);
        let egress = HeadEgress::new(merged);
        let err = egress
            .subscribe("logs", None)
            .err()
            .expect("logs is not supported yet");
        assert!(matches!(err, EgressError::UnsupportedMethod(m) if m == "logs"));
    }
}
