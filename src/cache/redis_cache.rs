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

//! Redis storage for cached blocks, transactions, and receipts.
//!
//! Redis extends the in-memory caches for `Requested`-tagged data: the
//! memory layer absorbs the hot recent data, while Redis holds more history
//! and survives restarts (and can be shared between instances). Keys and
//! values are compatible with the legacy implementation:
//! `block:{chain_id}:{hash}`, `tx:{chain_id}:{hash}`,
//! `tx-receipt:{chain_id}:{hash}`, with protobuf-encoded values (see
//! [`redis_proto`](super::redis_proto)).
//!
//! Every entry has a TTL derived from how settled the data is: a block is
//! kept for its own age (a minute-old block may still reorg, so it is kept
//! a minute; an old block is safe to keep the maximum time), a transaction
//! follows its block, and a receipt is kept in proportion to its
//! confirmations.
//!
//! Writes and evictions are fire-and-forget background tasks — a Redis
//! failure must never fail or delay the request that triggered it. Reads
//! treat any failure as a cache miss.

use crate::cache::redis_proto;
use crate::config::cache::RedisConfig;
use crate::data::{BlockContainer, BlockId, TxContainer, TxId};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

/// A block younger than this can still be replaced by a reorg — not worth
/// storing in Redis at all.
const MIN_BLOCK_TTL_SECONDS: i64 = 3;

/// Maximum time to keep a block.
const MAX_BLOCK_TTL_SECONDS: u64 = 60 * 60;

/// Maximum time to keep a transaction or a receipt.
const MAX_TX_TTL_SECONDS: u64 = 24 * 60 * 60;

/// Fallback receipt TTL when the confirmation count is unknown.
const DEFAULT_RECEIPT_TTL_SECONDS: u64 = 30;

/// Assumed block interval for converting confirmations into a TTL.
const BLOCK_TIME_SECONDS: u64 = 10;

/// Establish the shared Redis connection, verifying it with a `PING`.
///
/// One connection is shared by all chains — the keys are namespaced by the
/// chain id. The returned manager reconnects automatically; a failure here
/// means Redis is misconfigured and startup should be aborted (matching the
/// legacy behavior).
pub async fn connect(config: &RedisConfig) -> anyhow::Result<ConnectionManager> {
    // Deliberately not including the password in the logged address
    tracing::info!(
        "Use Redis cache at: redis://{}:{}/{}",
        config.host,
        config.port,
        config.db
    );
    let mut redis_settings = redis::RedisConnectionInfo::default().set_db(config.db as i64);
    if let Some(password) = &config.password {
        redis_settings = redis_settings.set_password(password);
    }
    let info = format!("redis://{}:{}", config.host, config.port)
        .parse::<redis::ConnectionInfo>()?
        .set_redis_settings(redis_settings);
    let client = redis::Client::open(info)?;
    let mut conn = ConnectionManager::new(client).await?;
    let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
    anyhow::ensure!(pong == "PONG", "unexpected Redis PING response: {pong}");
    tracing::info!("Connection to Redis established");
    Ok(conn)
}

/// Per-chain view of the Redis storage.
#[derive(Clone)]
pub struct RedisCache {
    conn: ConnectionManager,
    chain_id: i32,
}

impl RedisCache {
    pub fn new(conn: ConnectionManager, chain_id: i32) -> Self {
        Self { conn, chain_id }
    }

    fn block_key(&self, id: &BlockId) -> String {
        format!("block:{}:{}", self.chain_id, id.to_hex())
    }

    fn tx_key(&self, id: &TxId) -> String {
        format!("tx:{}:{}", self.chain_id, id.to_hex())
    }

    fn receipt_key(&self, id: &TxId) -> String {
        format!("tx-receipt:{}:{}", self.chain_id, id.to_hex())
    }

    // ─── Reads ─────────────────────────────────────────────────────────────

    pub async fn read_block(&self, id: &BlockId) -> Option<BlockContainer> {
        let data = self.get(self.block_key(id)).await?;
        redis_proto::decode_block(&data)
    }

    pub async fn read_tx(&self, id: &TxId) -> Option<TxContainer> {
        let data = self.get(self.tx_key(id)).await?;
        redis_proto::decode_tx(&data, redis_proto::ValueType::Tx)
    }

    pub async fn read_receipt(&self, id: &TxId) -> Option<TxContainer> {
        let data = self.get(self.receipt_key(id)).await?;
        redis_proto::decode_tx(&data, redis_proto::ValueType::TxReceipt)
    }

    async fn get(&self, key: String) -> Option<Vec<u8>> {
        let mut conn = self.conn.clone();
        match conn.get::<_, Option<Vec<u8>>>(&key).await {
            Ok(value) => value,
            Err(e) => {
                tracing::debug!(key, error = %e, "redis read failed");
                None
            }
        }
    }

    // ─── Writes ────────────────────────────────────────────────────────────

    /// Store a block, with a TTL matching its age.
    pub async fn put_block(&self, block: &BlockContainer) {
        let Some(ttl) = block_ttl(block.timestamp, jiff::Timestamp::now()) else {
            return;
        };
        let Some(data) = redis_proto::encode_block(block) else {
            return;
        };
        self.set(self.block_key(&block.hash), data, ttl).await;
    }

    /// Store a transaction, with a TTL matching its block's age.
    pub async fn put_tx(&self, tx: &TxContainer, block_timestamp: jiff::Timestamp) {
        let Some(ttl) = tx_ttl(block_timestamp, jiff::Timestamp::now()) else {
            return;
        };
        let Some(data) = redis_proto::encode_tx(tx, redis_proto::ValueType::Tx) else {
            return;
        };
        self.set(self.tx_key(&tx.hash), data, ttl).await;
    }

    /// Store a receipt, with a TTL proportional to its confirmations.
    pub async fn put_receipt(&self, receipt: &TxContainer, current_height: Option<u64>) {
        let ttl = receipt_ttl(receipt.height, current_height);
        let Some(data) = redis_proto::encode_tx(receipt, redis_proto::ValueType::TxReceipt) else {
            return;
        };
        self.set(self.receipt_key(&receipt.hash), data, ttl).await;
    }

    async fn set(&self, key: String, value: Vec<u8>, ttl_seconds: u64) {
        let mut conn = self.conn.clone();
        if let Err(e) = conn.set_ex::<_, _, ()>(&key, value, ttl_seconds).await {
            tracing::warn!(key, error = %e, "failed to save to redis");
        }
    }

    // ─── Background wrappers ───────────────────────────────────────────────
    //
    // The synchronous cache layer must not wait for Redis IO, so writes and
    // evictions are spawned onto the runtime and the outcome is only logged.

    pub fn write_block(&self, block: BlockContainer) {
        let redis = self.clone();
        tokio::spawn(async move { redis.put_block(&block).await });
    }

    pub fn write_tx(&self, tx: TxContainer, block_timestamp: jiff::Timestamp) {
        let redis = self.clone();
        tokio::spawn(async move { redis.put_tx(&tx, block_timestamp).await });
    }

    pub fn write_receipt(&self, receipt: TxContainer, current_height: Option<u64>) {
        let redis = self.clone();
        tokio::spawn(async move { redis.put_receipt(&receipt, current_height).await });
    }

    pub fn evict_block(&self, id: &BlockId) {
        self.delete(vec![self.block_key(id)]);
    }

    pub fn evict_txs(&self, txs: &[TxId]) {
        self.delete(txs.iter().map(|id| self.tx_key(id)).collect());
    }

    pub fn evict_receipts(&self, txs: &[TxId]) {
        self.delete(txs.iter().map(|id| self.receipt_key(id)).collect());
    }

    fn delete(&self, keys: Vec<String>) {
        if keys.is_empty() {
            return;
        }
        let mut conn = self.conn.clone();
        tokio::spawn(async move {
            if let Err(e) = conn.del::<_, ()>(&keys).await {
                tracing::warn!(error = %e, "failed to delete from redis");
            }
        });
    }
}

// ─── TTL rules ───────────────────────────────────────────────────────────────

/// A block is kept for as long as its current age: a block that survived an
/// hour is settled and can be kept the maximum time, while a seconds-old
/// block may still be replaced and is not stored at all.
fn block_ttl(block_time: jiff::Timestamp, now: jiff::Timestamp) -> Option<u64> {
    let age = now.as_second().saturating_sub(block_time.as_second());
    if age <= MIN_BLOCK_TTL_SECONDS {
        return None;
    }
    Some((age as u64).min(MAX_BLOCK_TTL_SECONDS))
}

/// A transaction is as settled as the block that includes it, so it follows
/// the same age rule, just with a longer maximum (the tx content, unlike the
/// block at a height, can't change — only disappear on a reorg).
fn tx_ttl(block_time: jiff::Timestamp, now: jiff::Timestamp) -> Option<u64> {
    let age = now.as_second().saturating_sub(block_time.as_second());
    if age <= 0 {
        return None;
    }
    Some((age as u64).min(MAX_TX_TTL_SECONDS))
}

/// A receipt is kept in proportion to its confirmations; without a known
/// confirmation count it is kept only briefly.
fn receipt_ttl(height: Option<u64>, current_height: Option<u64>) -> u64 {
    match (height, current_height) {
        (Some(height), Some(current)) if current > height => {
            ((current - height) * BLOCK_TIME_SECONDS).min(MAX_TX_TTL_SECONDS)
        }
        _ => DEFAULT_RECEIPT_TTL_SECONDS,
    }
}

#[cfg(test)]
mod ttl_tests {
    use super::*;
    use jiff::Timestamp;

    fn ts(second: i64) -> Timestamp {
        Timestamp::from_second(second).unwrap()
    }

    #[test]
    fn fresh_block_not_stored() {
        assert_eq!(block_ttl(ts(1000), ts(1000)), None);
        assert_eq!(block_ttl(ts(1000), ts(1003)), None);
    }

    #[test]
    fn block_kept_for_its_age() {
        assert_eq!(block_ttl(ts(1000), ts(1060)), Some(60));
    }

    #[test]
    fn old_block_capped_at_an_hour() {
        assert_eq!(block_ttl(ts(0), ts(100_000)), Some(3600));
    }

    #[test]
    fn tx_kept_for_block_age_capped_at_a_day() {
        assert_eq!(tx_ttl(ts(1000), ts(1000)), None);
        assert_eq!(tx_ttl(ts(1000), ts(1060)), Some(60));
        assert_eq!(tx_ttl(ts(0), ts(1_000_000)), Some(86_400));
    }

    #[test]
    fn receipt_ttl_from_confirmations() {
        assert_eq!(receipt_ttl(Some(100), Some(106)), 60);
        assert_eq!(receipt_ttl(Some(0), Some(1_000_000)), 86_400);
    }

    #[test]
    fn receipt_ttl_default_without_confirmations() {
        assert_eq!(receipt_ttl(Some(100), Some(100)), 30);
        assert_eq!(receipt_ttl(Some(100), Some(90)), 30);
        assert_eq!(receipt_ttl(None, Some(100)), 30);
        assert_eq!(receipt_ttl(Some(100), None), 30);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::cache::{CacheTag, Caches};
    use std::sync::Arc;
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{ContainerAsync, GenericImage};

    const CHAIN_ID: i32 = 100;

    async fn start_redis() -> (ContainerAsync<GenericImage>, ConnectionManager) {
        let container = GenericImage::new("redis", "7-alpine")
            .with_exposed_port(6379.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await
            .expect("redis container must start (is Docker running?)");
        let port = container.get_host_port_ipv4(6379.tcp()).await.unwrap();
        let config = RedisConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port,
            db: 0,
            password: None,
        };
        let conn = connect(&config).await.unwrap();
        (container, conn)
    }

    /// An hour-old block — old enough to pass every TTL rule.
    fn settled_block(hash_byte: u8, height: u64) -> BlockContainer {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
        BlockContainer {
            hash: BlockId::from_bytes(hash),
            height,
            parent_hash: None,
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::now() - jiff::Span::new().hours(1),
            transaction_hashes: vec![],
            json: Some(Arc::from(br#"{"number":"0x64"}"#.as_slice())),
            header_json: None,
        }
    }

    fn tx_of(block: &BlockContainer, hash_byte: u8) -> TxContainer {
        let mut hash = [0u8; 32];
        hash[0] = hash_byte;
        TxContainer {
            hash: TxId::from_bytes(hash),
            block_hash: Some(block.hash),
            height: Some(block.height),
            json: Some(Arc::from(br#"{"hash":"0xaa"}"#.as_slice())),
        }
    }

    #[tokio::test]
    async fn block_roundtrip_with_ttl() {
        let (_container, conn) = start_redis().await;
        let redis = RedisCache::new(conn.clone(), CHAIN_ID);
        let block = settled_block(1, 100);

        redis.put_block(&block).await;

        let read = redis.read_block(&block.hash).await.unwrap();
        assert_eq!(read.height, 100);
        assert_eq!(read.json.as_deref(), block.json.as_deref());

        // The key must carry the legacy-compatible name and a TTL
        let mut conn = conn.clone();
        let key = format!("block:{}:{}", CHAIN_ID, block.hash.to_hex());
        let ttl: i64 = redis::cmd("TTL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!((3590..=3600).contains(&ttl), "unexpected TTL: {ttl}");
    }

    #[tokio::test]
    async fn fresh_block_not_written() {
        let (_container, conn) = start_redis().await;
        let redis = RedisCache::new(conn, CHAIN_ID);
        let mut block = settled_block(1, 100);
        block.timestamp = jiff::Timestamp::now();

        redis.put_block(&block).await;

        assert!(redis.read_block(&block.hash).await.is_none());
    }

    #[tokio::test]
    async fn tx_and_receipt_roundtrip() {
        let (_container, conn) = start_redis().await;
        let redis = RedisCache::new(conn, CHAIN_ID);
        let block = settled_block(1, 100);
        let tx = tx_of(&block, 0xaa);

        redis.put_tx(&tx, block.timestamp).await;
        redis.put_receipt(&tx, Some(106)).await;

        assert_eq!(redis.read_tx(&tx.hash).await.unwrap().hash, tx.hash);
        assert!(redis.read_receipt(&tx.hash).await.is_some());
        // tx and receipt live under different keys
        assert!(redis.read_tx(&TxId::from_bytes([0xbb; 32])).await.is_none());
    }

    #[tokio::test]
    async fn caches_read_through_redis() {
        let (_container, conn) = start_redis().await;
        let block = settled_block(1, 100);

        // One instance caches a requested block...
        let writer = Caches::with_redis(Some(RedisCache::new(conn.clone(), CHAIN_ID)));
        writer.cache(CacheTag::Requested, block.clone());
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // ...and another instance (cold memory) finds it via Redis
        let reader = Caches::with_redis(Some(RedisCache::new(conn, CHAIN_ID)));
        let read = reader.read_block_by_hash(&block.hash).await.unwrap();
        assert_eq!(read.height, 100);
        assert!(
            reader.get_block_by_hash(&block.hash).is_none(),
            "must not be in memory"
        );
    }

    #[tokio::test]
    async fn eviction_removes_block_and_dependents() {
        let (_container, conn) = start_redis().await;
        let redis = RedisCache::new(conn, CHAIN_ID);
        let block = settled_block(1, 100);
        let tx = tx_of(&block, 0xaa);

        redis.put_block(&block).await;
        redis.put_tx(&tx, block.timestamp).await;
        redis.put_receipt(&tx, Some(106)).await;

        redis.evict_block(&block.hash);
        redis.evict_txs(&[tx.hash]);
        redis.evict_receipts(&[tx.hash]);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(redis.read_block(&block.hash).await.is_none());
        assert!(redis.read_tx(&tx.hash).await.is_none());
        assert!(redis.read_receipt(&tx.hash).await.is_none());
    }
}
