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

//! Transaction status tracking for the gRPC `SubscribeTxStatus` method.
//!
//! A request names a chain and a transaction id; the stream reports the
//! transaction's progress — broadcast (seen) → mined → confirmations — emitting
//! a new `TxStatus` whenever the state changes, and completing once the
//! requested confirmation count is reached. Ports the legacy `TrackEthereumTx` /
//! `TrackBitcoinTx` reactive state machines onto a shared, chain-agnostic driver
//! that re-reads the transaction on each new head (plus a poll while it isn't
//! mined yet) and applies time-to-live cutoffs.

pub mod bitcoin;
pub mod ethereum;

use crate::data::BlockContainer;
use crate::upstream::merged_head::MergedHead;
use emerald_api::proto::blockchain::TxStatus;
use emerald_api::proto::common::BlockInfo;
use futures::stream::{self, Stream};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::Instant;

/// A stream of `TxStatus` updates, matching the gRPC handler's stream type.
pub type TxStatusStream = Pin<Box<dyn Stream<Item = Result<TxStatus, tonic::Status>> + Send>>;

/// How often the tracker re-reads a transaction that isn't mined yet (waiting
/// for broadcast or inclusion). Once mined, updates are head-driven. Legacy
/// retries every 1–2s while waiting.
const NOT_MINED_POLL: Duration = Duration::from_secs(2);

/// A slow fallback re-read for a mined transaction on a chain with no head
/// stream, so confirmations still advance.
const MINED_FALLBACK_POLL: Duration = Duration::from_secs(30);

/// Why a tx-status request can't be served.
#[derive(Debug, PartialEq, Eq)]
pub enum TxStatusError {
    /// No upstream is configured for the requested chain.
    Unavailable(i32),
    /// The chain doesn't support transaction status tracking.
    Unsupported,
}

impl TxStatusError {
    pub fn into_status(self) -> tonic::Status {
        match self {
            TxStatusError::Unavailable(chain) => {
                tonic::Status::unavailable(format!("BLOCKCHAIN UNAVAILABLE: {chain}"))
            }
            TxStatusError::Unsupported => {
                tonic::Status::unimplemented("transaction status is not supported for this chain")
            }
        }
    }
}

/// The on-chain state of a transaction at a point in time. Confirmations are
/// computed by the reader from the chain head (`head_height - tx_height + 1`).
#[derive(Clone, PartialEq, Eq)]
pub struct TxState {
    /// Seen by the node (in the mempool or a block) — proto `broadcasted`.
    pub found: bool,
    /// Included in a block.
    pub mined: bool,
    /// Height of the including block, once mined.
    pub height: Option<u64>,
    /// Hash of the including block (chain-native hex), once mined.
    pub block_id: Option<String>,
    /// Timestamp of the including block in epoch milliseconds, once mined.
    pub block_time: Option<u64>,
    /// Confirmations so far (`0` until mined).
    pub confirmations: u32,
}

impl TxState {
    /// A transaction the node hasn't seen yet.
    pub fn not_found() -> Self {
        Self {
            found: false,
            mined: false,
            height: None,
            block_id: None,
            block_time: None,
            confirmations: 0,
        }
    }

    /// A transaction seen (broadcast) but not yet in a block.
    pub fn pending() -> Self {
        Self {
            found: true,
            ..Self::not_found()
        }
    }

    fn to_proto(&self, tx_id: &str) -> TxStatus {
        TxStatus {
            tx_id: tx_id.to_string(),
            broadcasted: self.found,
            mined: self.mined,
            block: self.mined.then(|| BlockInfo {
                height: self.height.unwrap_or(0),
                block_id: self.block_id.clone().unwrap_or_default(),
                timestamp: self.block_time.unwrap_or(0),
            }),
            confirmations: self.confirmations,
        }
    }
}

/// Confirmations for a transaction at `tx_height` given the chain head, never
/// below 1 once mined (a head briefly behind the tx still counts as one).
pub fn confirmations(head_height: Option<u64>, tx_height: u64) -> u32 {
    match head_height {
        Some(head) if head >= tx_height => (head - tx_height + 1) as u32,
        _ => 1,
    }
}

/// Reads a transaction's current on-chain state. Implemented per chain.
#[async_trait::async_trait]
pub trait TxReader: Send + Sync {
    /// Read the transaction, using the chain head for confirmation counting.
    /// `found: false` when the node hasn't seen it yet; `Err` only on a genuine
    /// read failure (so the stream fails rather than reporting a stale state).
    async fn read(&self) -> Result<TxState, tonic::Status>;

    /// The transaction id echoed in responses (chain-canonical form).
    fn tx_id(&self) -> &str;
}

/// Time-to-live cutoffs that complete the subscription (legacy `shouldClose`).
/// A `None` means "no cutoff for this condition".
pub struct Ttl {
    /// Overall lifetime regardless of state.
    pub overall: Option<Duration>,
    /// Lifetime while the transaction has never been seen.
    pub not_found: Option<Duration>,
    /// Lifetime while seen but not yet mined.
    pub not_mined: Option<Duration>,
}

/// Drive a `SubscribeTxStatus` subscription: emit the initial state, then re-read
/// on each new head (and on a poll while the transaction isn't mined yet),
/// emitting on every change and completing when `limit` confirmations are
/// reached or a [`Ttl`] cutoff fires.
pub fn subscribe(
    head: Option<Arc<MergedHead>>,
    reader: Arc<dyn TxReader>,
    limit: u32,
    ttl: Ttl,
) -> TxStatusStream {
    struct State {
        reader: Arc<dyn TxReader>,
        head: Option<broadcast::Receiver<Arc<BlockContainer>>>,
        last: Option<TxState>,
        start: Instant,
        limit: u32,
        ttl: Ttl,
        primed: bool,
        done: bool,
    }

    let state = State {
        reader,
        head: head.map(|h| h.subscribe()),
        last: None,
        start: Instant::now(),
        limit,
        ttl,
        primed: false,
        done: false,
    };

    let stream = stream::unfold(state, |mut state| async move {
        loop {
            if state.done {
                return None;
            }
            // After the first read, wait for a trigger (new head or poll tick);
            // a TTL deadline reached while waiting completes the stream.
            if state.primed {
                let deadline = ttl_deadline(state.start, state.last.as_ref(), &state.ttl);
                // Poll fast until mined; once mined rely on heads (or a slow
                // fallback when the chain has no head stream).
                let poll = match &state.last {
                    Some(tx) if tx.mined => {
                        if state.head.is_some() {
                            None
                        } else {
                            Some(MINED_FALLBACK_POLL)
                        }
                    }
                    _ => Some(NOT_MINED_POLL),
                };
                let expired = tokio::select! {
                    _ = recv_head(&mut state.head) => false,
                    _ = sleep_opt(poll) => false,
                    _ = sleep_until_opt(deadline) => true,
                };
                if expired {
                    return None;
                }
            } else {
                state.primed = true;
            }

            let tx = match state.reader.read().await {
                Ok(tx) => tx,
                Err(status) => {
                    state.done = true;
                    return Some((Err(status), state));
                }
            };

            let reached_limit = tx.mined && tx.confirmations >= state.limit;
            if state.last.as_ref() != Some(&tx) {
                state.last = Some(tx.clone());
                if reached_limit {
                    state.done = true;
                }
                return Some((Ok(tx.to_proto(state.reader.tx_id())), state));
            }
            // Unchanged: complete if already at the limit, else wait again.
            if reached_limit {
                return None;
            }
        }
    });
    Box::pin(stream)
}

/// Await the next head block, or never (a chain with no head stream relies on
/// the poll branch instead).
async fn recv_head(head: &mut Option<broadcast::Receiver<Arc<BlockContainer>>>) {
    match head {
        // A lagged/closed channel still counts as a wake — the re-read decides
        // what to do next.
        Some(rx) => {
            let _ = rx.recv().await;
        }
        None => std::future::pending().await,
    }
}

/// Sleep for `duration`, or never when it's `None`.
async fn sleep_opt(duration: Option<Duration>) {
    match duration {
        Some(duration) => tokio::time::sleep(duration).await,
        None => std::future::pending().await,
    }
}

/// Sleep until `deadline`, or never when it's `None`.
async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// The earliest TTL deadline that applies given the last observed state (legacy
/// `shouldClose`): the overall lifetime always, plus the not-found or not-mined
/// cutoff depending on how far the transaction has progressed.
fn ttl_deadline(start: Instant, last: Option<&TxState>, ttl: &Ttl) -> Option<Instant> {
    let mut deadline: Option<Instant> = ttl.overall.map(|d| start + d);
    let mut consider = |d: Option<Duration>| {
        if let Some(at) = d.map(|d| start + d) {
            deadline = Some(deadline.map_or(at, |cur| cur.min(at)));
        }
    };
    match last {
        Some(tx) if !tx.found => consider(ttl.not_found),
        Some(tx) if !tx.mined => consider(ttl.not_mined),
        _ => {}
    }
    deadline
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{BlockContainer, BlockId};
    use crate::upstream::head::CurrentHead;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use tokio_stream::StreamExt;

    /// A reader that returns states from a script, repeating the last once the
    /// script is exhausted (so head-triggered re-reads keep working).
    struct ScriptReader {
        states: Mutex<VecDeque<TxState>>,
        last: Mutex<TxState>,
    }

    impl ScriptReader {
        fn new(states: Vec<TxState>) -> Arc<Self> {
            Arc::new(Self {
                states: Mutex::new(states.into()),
                last: Mutex::new(TxState::not_found()),
            })
        }
    }

    #[async_trait::async_trait]
    impl TxReader for ScriptReader {
        fn tx_id(&self) -> &str {
            "deadbeef"
        }
        async fn read(&self) -> Result<TxState, tonic::Status> {
            let next = self
                .states
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| self.last.lock().unwrap().clone());
            *self.last.lock().unwrap() = next.clone();
            Ok(next)
        }
    }

    fn mined(confirmations: u32) -> TxState {
        TxState {
            found: true,
            mined: true,
            height: Some(100),
            block_id: Some("bb".into()),
            block_time: Some(1_700_000_000_000),
            confirmations,
        }
    }

    fn no_ttl() -> Ttl {
        Ttl {
            overall: None,
            not_found: None,
            not_mined: None,
        }
    }

    fn block(height: u64) -> BlockContainer {
        BlockContainer {
            hash: BlockId::from_bytes([height as u8; 32]),
            height,
            parent_hash: None,
            total_difficulty: alloy::primitives::U256::ZERO,
            timestamp: jiff::Timestamp::UNIX_EPOCH,
            transaction_hashes: vec![],
            json: None,
            header_json: None,
        }
    }

    // ── pure helpers ─────────────────────────────────────────────────────

    #[test]
    fn confirmations_count_from_head() {
        assert_eq!(confirmations(Some(105), 100), 6);
        // A head briefly behind the tx still counts as one confirmation.
        assert_eq!(confirmations(Some(99), 100), 1);
        assert_eq!(confirmations(None, 100), 1);
    }

    #[test]
    fn proto_block_only_present_when_mined() {
        let pending = TxState::pending().to_proto("ab");
        assert!(pending.broadcasted);
        assert!(!pending.mined);
        assert!(pending.block.is_none());

        let mined = mined(3).to_proto("ab");
        assert!(mined.mined);
        let block = mined.block.unwrap();
        assert_eq!(block.height, 100);
        assert_eq!(block.block_id, "bb");
        assert_eq!(mined.confirmations, 3);
        assert_eq!(mined.tx_id, "ab");
    }

    // ── driver ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn emits_initial_then_completes_at_limit() {
        let reader = ScriptReader::new(vec![mined(1), mined(2)]);
        let current = Arc::new(CurrentHead::new());
        let head = MergedHead::new(vec![Arc::clone(&current)]);
        let mut stream = subscribe(Some(head), reader, 2, no_ttl());

        // Initial state.
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.confirmations, 1);
        assert!(first.mined);

        // A new head triggers a re-read returning the limit; emitted, then done.
        current.update_with_block(block(10));
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.confirmations, 2);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn not_found_ttl_completes_the_stream() {
        let reader = ScriptReader::new(vec![TxState::not_found()]);
        let ttl = Ttl {
            overall: None,
            not_found: Some(Duration::from_millis(20)),
            not_mined: None,
        };
        // No head: the only progress is the TTL.
        let mut stream = subscribe(None, reader, 6, ttl);

        let first = stream.next().await.unwrap().unwrap();
        assert!(!first.broadcasted);

        let next = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("stream should complete after the not-found TTL");
        assert!(next.is_none());
    }
}
