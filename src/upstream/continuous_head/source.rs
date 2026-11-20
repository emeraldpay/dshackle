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

//! Where the continuous head gets the blocks the announced heads skipped.

use crate::blockchain::BlockchainType;
use crate::data::{BlockContainer, BlockId};
use crate::jsonrpc::{JsonRpcRequest, RpcMethod};
use crate::upstream::bitcoin::head::parse_btc_block;
use crate::upstream::egress::{ChainAccess, fetch_block_json};
use crate::upstream::ethereum::head::parse_eth_block;
use std::sync::Arc;

/// Looks up a block by its hash.
#[async_trait::async_trait]
pub trait BlockSource: Send + Sync {
    /// The block, or `None` when it can't be found. A miss is not an error:
    /// the caller announces what it has without the block.
    ///
    /// `height` is where the block is expected, so the lookup can go to an
    /// upstream that has reached it: a fresh block, or one of a fresh fork, is
    /// unknown to an upstream a block or two behind.
    async fn block_by_hash(&self, hash: &BlockId, height: u64) -> Option<BlockContainer>;
}

/// The block source of a chain family, reading through the chain's routing
/// (and so its caches). `None` for a family without a known block lookup.
pub fn routed(
    family: BlockchainType,
    access: Arc<dyn ChainAccess>,
) -> Option<Arc<dyn BlockSource>> {
    match family {
        BlockchainType::Ethereum => Some(Arc::new(EthereumBlocks(access))),
        BlockchainType::Bitcoin => Some(Arc::new(BitcoinBlocks(access))),
        BlockchainType::Unknown => None,
    }
}

/// Blocks by `eth_getBlockByHash`. Its routing already moves on from an
/// upstream answering `null` (a non-empty quorum), so the expected height
/// isn't needed to find one that has the block.
pub struct EthereumBlocks(pub Arc<dyn ChainAccess>);

#[async_trait::async_trait]
impl BlockSource for EthereumBlocks {
    async fn block_by_hash(&self, hash: &BlockId, _height: u64) -> Option<BlockContainer> {
        let raw = fetch_block_json(&*self.0, hash).await?;
        parse_eth_block(std::str::from_utf8(&raw).ok()?)
    }
}

/// Blocks by `getblock` (verbosity 1: with transaction ids).
///
/// Routed to an upstream that has reached the block's height: `getblock`
/// takes the first answer it gets, and a node slightly behind answers "Block
/// not found" for exactly the blocks the walk asks for.
pub struct BitcoinBlocks(pub Arc<dyn ChainAccess>);

#[async_trait::async_trait]
impl BlockSource for BitcoinBlocks {
    async fn block_by_hash(&self, hash: &BlockId, height: u64) -> Option<BlockContainer> {
        let request = JsonRpcRequest::new(
            0,
            RpcMethod::from("getblock"),
            serde_json::json!([hash.to_hex(), 1]),
        );
        let response = self.0.call_at_height(&request, height).await.ok()?;
        if response.error.is_some() {
            return None;
        }
        parse_btc_block(response.result?.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::JsonRpcResponse;
    use crate::upstream::traits::UpstreamError;
    use std::sync::Mutex;

    const ETH_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    const ETH_PARENT: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";
    const BTC_HASH: &str = "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72f8804c6";
    const BTC_PARENT: &str = "00000000000000000001deadbeef00000000000000000000000000000000beef";

    /// Answers every call with one canned JSON-RPC body (or a transport
    /// failure), remembering the last request.
    struct CannedChain {
        body: Option<String>,
        last: Mutex<Option<JsonRpcRequest>>,
        /// The `min_height` of the last call routed by height.
        routed_at: Mutex<Option<u64>>,
    }

    impl CannedChain {
        fn answering(body: &str) -> Arc<Self> {
            Arc::new(Self {
                body: Some(body.to_string()),
                last: Mutex::new(None),
                routed_at: Mutex::new(None),
            })
        }

        fn result(result: &str) -> Arc<Self> {
            Self::answering(&format!(r#"{{"jsonrpc":"2.0","id":1,"result":{result}}}"#))
        }

        fn unreachable() -> Arc<Self> {
            Arc::new(Self {
                body: None,
                last: Mutex::new(None),
                routed_at: Mutex::new(None),
            })
        }

        fn last_request(&self) -> JsonRpcRequest {
            self.last.lock().unwrap().clone().expect("no call made")
        }
    }

    #[async_trait::async_trait]
    impl ChainAccess for CannedChain {
        fn is_syncing(&self) -> bool {
            false
        }

        async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            *self.last.lock().unwrap() = Some(request.clone());
            match &self.body {
                Some(body) => Ok(serde_json::from_str(body).unwrap()),
                None => Err(UpstreamError::Transport("connection refused".into())),
            }
        }

        async fn call_at_height(
            &self,
            request: &JsonRpcRequest,
            min_height: u64,
        ) -> Result<JsonRpcResponse, UpstreamError> {
            *self.routed_at.lock().unwrap() = Some(min_height);
            self.call(request).await
        }
    }

    fn eth_block() -> String {
        format!(
            r#"{{"hash":"{ETH_HASH}","number":"0x64","parentHash":"{ETH_PARENT}","timestamp":"0x5d5d4a0c","transactions":[]}}"#
        )
    }

    fn btc_block() -> String {
        format!(
            r#"{{"hash":"{BTC_HASH}","height":800000,"time":1690000000,"previousblockhash":"{BTC_PARENT}","tx":[]}}"#
        )
    }

    #[tokio::test]
    async fn ethereum_block_is_read_by_hash() {
        let chain = CannedChain::result(&eth_block());
        let source = EthereumBlocks(Arc::clone(&chain) as _);

        let block = source
            .block_by_hash(&ETH_HASH.parse().unwrap(), 100)
            .await
            .unwrap();

        assert_eq!(block.height, 100);
        assert_eq!(block.parent_hash, Some(ETH_PARENT.parse().unwrap()));
        let request = chain.last_request();
        assert_eq!(request.method.as_ref(), "eth_getBlockByHash");
        assert_eq!(request.params, serde_json::json!([ETH_HASH, false]));
    }

    #[tokio::test]
    async fn bitcoin_block_is_read_by_hash() {
        let chain = CannedChain::result(&btc_block());
        let source = BitcoinBlocks(Arc::clone(&chain) as _);

        let block = source
            .block_by_hash(&BTC_HASH.parse().unwrap(), 800_000)
            .await
            .unwrap();

        assert_eq!(block.height, 800_000);
        assert_eq!(block.parent_hash, Some(BTC_PARENT.parse().unwrap()));
        let request = chain.last_request();
        assert_eq!(request.method.as_ref(), "getblock");
        // Bitcoin hashes go without the `0x` prefix; verbosity 1 lists tx ids.
        assert_eq!(request.params, serde_json::json!([BTC_HASH, 1]));
        assert_eq!(*chain.routed_at.lock().unwrap(), Some(800_000));
    }

    #[tokio::test]
    async fn unknown_block_is_none() {
        for source in [
            Box::new(EthereumBlocks(CannedChain::result("null"))) as Box<dyn BlockSource>,
            Box::new(BitcoinBlocks(CannedChain::result("null"))),
        ] {
            assert!(
                source
                    .block_by_hash(&ETH_HASH.parse().unwrap(), 100)
                    .await
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn rpc_error_is_none() {
        let error = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-5,"message":"Block not found"}}"#;
        for source in [
            Box::new(EthereumBlocks(CannedChain::answering(error))) as Box<dyn BlockSource>,
            Box::new(BitcoinBlocks(CannedChain::answering(error))),
        ] {
            assert!(
                source
                    .block_by_hash(&ETH_HASH.parse().unwrap(), 100)
                    .await
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn failed_call_is_none() {
        for source in [
            Box::new(EthereumBlocks(CannedChain::unreachable())) as Box<dyn BlockSource>,
            Box::new(BitcoinBlocks(CannedChain::unreachable())),
        ] {
            assert!(
                source
                    .block_by_hash(&ETH_HASH.parse().unwrap(), 100)
                    .await
                    .is_none()
            );
        }
    }

    #[test]
    fn unknown_family_has_no_source() {
        assert!(routed(BlockchainType::Unknown, CannedChain::unreachable()).is_none());
        assert!(routed(BlockchainType::Ethereum, CannedChain::unreachable()).is_some());
        assert!(routed(BlockchainType::Bitcoin, CannedChain::unreachable()).is_some());
    }
}
