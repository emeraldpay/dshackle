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

//! Fee estimation for the gRPC `EstimateFee` method.
//!
//! Samples one transaction per block over a recent window and aggregates their
//! fees. The transaction position within each block and the way the per-block
//! values are combined are both driven by the requested [`FeeMode`], mirroring
//! the legacy `AbstractChainFees` / `ChainFees` hierarchy.
//!
//! Ethereum-family chains read per-transaction fee fields (see [`ethereum`]);
//! Bitcoin derives a fee from input/output amount differences (see [`bitcoin`]).

mod bitcoin;
mod ethereum;

pub use bitcoin::BitcoinFees;
pub use ethereum::EthereumFees;

use emerald_api::proto::blockchain::EstimateFeeResponse;

/// How the fee is estimated. Mirrors the legacy `ChainFees.Mode`; the proto
/// `INVALID`/unrecognized values map to "no mode" (a request error).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeeMode {
    /// Average over the last transaction of each block.
    AvgLast,
    /// Average over the 5th-from-last transaction of each block.
    AvgT5,
    /// Average over the 20th-from-last transaction of each block.
    AvgT20,
    /// Average over the 50th-from-last transaction of each block.
    AvgT50,
    /// Highest fee across the sampled blocks (a fee accepted by every block).
    MinAlways,
    /// Average over the middle transaction of each block.
    AvgMiddle,
    /// Average over the first (top) transaction of each block.
    AvgTop,
}

impl FeeMode {
    /// Map the proto `FeeEstimationMode` to a [`FeeMode`], or `None` for
    /// `INVALID`/unrecognized values (legacy `ChainFees.extractMode`).
    pub fn from_proto(mode: i32) -> Option<FeeMode> {
        use emerald_api::proto::blockchain::FeeEstimationMode as M;
        match M::try_from(mode).ok()? {
            M::AvgLast => Some(FeeMode::AvgLast),
            M::AvgT5 => Some(FeeMode::AvgT5),
            M::AvgT20 => Some(FeeMode::AvgT20),
            M::AvgT50 => Some(FeeMode::AvgT50),
            M::MinAlways => Some(FeeMode::MinAlways),
            M::AvgMiddle => Some(FeeMode::AvgMiddle),
            M::AvgTop => Some(FeeMode::AvgTop),
            M::Invalid => None,
        }
    }

    /// Index of the transaction to sample in a block of `tx_count` transactions,
    /// or `None` when there is none to offer. Mirrors the legacy `TxAt`
    /// position selectors (`TxAtTop`/`TxAtBottom`/`TxAtMiddle`/`TxAtPos`).
    fn tx_index(self, tx_count: usize) -> Option<usize> {
        if tx_count == 0 {
            return None;
        }
        let index = match self {
            FeeMode::AvgTop => 0,
            FeeMode::MinAlways | FeeMode::AvgLast => tx_count - 1,
            FeeMode::AvgMiddle => {
                if tx_count == 1 {
                    0
                } else {
                    tx_count / 2
                }
            }
            FeeMode::AvgT5 => pos_from_end(tx_count, 5),
            FeeMode::AvgT20 => pos_from_end(tx_count, 20),
            FeeMode::AvgT50 => pos_from_end(tx_count, 50),
        };
        Some(index)
    }
}

/// Index of the transaction `pos` positions from the end, clamped to the block.
/// Legacy `TxAtPos`: `index = pos.coerceAtMost(size - 1)`, counted from the end.
fn pos_from_end(size: usize, pos: usize) -> usize {
    let from_end = pos.min(size - 1);
    size - from_end - 1
}

/// The block heights to sample for an estimate over the last `blocks`, clamped
/// to `height_limit` and to the chain length. Empty when the chain is shorter
/// than the window. Legacy `AbstractChainFees.usingBlocks`.
fn block_range(current_height: u64, blocks: u32, height_limit: u32) -> Vec<u64> {
    let use_blocks = blocks.clamp(1, height_limit) as u64;
    // The window can extend before genesis on a freshly started chain.
    let start = current_height as i64 - use_blocks as i64 + 1;
    if start < 0 {
        return Vec::new();
    }
    (start as u64..=current_height).collect()
}

/// Why a fee estimate could not be produced.
#[derive(Debug, PartialEq, Eq)]
pub enum FeeError {
    /// No upstream has reported a head yet, so there's no window to sample.
    NotReady,
    /// No usable transaction was found in the sampled window.
    NoData,
}

/// Fee estimator for one chain.
#[async_trait::async_trait]
pub trait ChainFees: Send + Sync {
    /// Estimate the current fee using `mode` over the last `blocks` blocks.
    async fn estimate(&self, mode: FeeMode, blocks: u32) -> Result<EstimateFeeResponse, FeeError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── tx position selection (legacy TxAt) ──────────────────────────────

    #[test]
    fn top_and_bottom_positions() {
        assert_eq!(FeeMode::AvgTop.tx_index(3), Some(0));
        assert_eq!(FeeMode::AvgLast.tx_index(3), Some(2));
        assert_eq!(FeeMode::MinAlways.tx_index(3), Some(2));
    }

    #[test]
    fn middle_position() {
        assert_eq!(FeeMode::AvgMiddle.tx_index(1), Some(0));
        assert_eq!(FeeMode::AvgMiddle.tx_index(2), Some(1));
        assert_eq!(FeeMode::AvgMiddle.tx_index(5), Some(2));
    }

    #[test]
    fn pos_from_end_is_clamped() {
        // Position 5 from the end of a 10-tx block → index 10 - 5 - 1 = 4
        // (legacy `TxAtPos`: `transactions[size - pos - 1]`).
        assert_eq!(FeeMode::AvgT5.tx_index(10), Some(4));
        // Block smaller than the position clamps to the first tx.
        assert_eq!(FeeMode::AvgT20.tx_index(3), Some(0));
        assert_eq!(FeeMode::AvgT50.tx_index(1), Some(0));
    }

    #[test]
    fn empty_block_has_no_tx() {
        assert_eq!(FeeMode::AvgTop.tx_index(0), None);
        assert_eq!(FeeMode::AvgT5.tx_index(0), None);
        assert_eq!(FeeMode::MinAlways.tx_index(0), None);
    }

    // ── block window (legacy usingBlocks) ────────────────────────────────

    #[test]
    fn range_spans_the_window_ending_at_head() {
        assert_eq!(block_range(100, 3, 256), vec![98, 99, 100]);
    }

    #[test]
    fn range_clamps_to_height_limit() {
        // 10 requested but the limit is 4 → only the last 4 blocks.
        assert_eq!(block_range(100, 10, 4), vec![97, 98, 99, 100]);
    }

    #[test]
    fn range_clamps_zero_blocks_to_one() {
        assert_eq!(block_range(100, 0, 256), vec![100]);
    }

    #[test]
    fn range_empty_when_chain_too_short() {
        // Window reaches before genesis (height 1, want 5 blocks).
        assert!(block_range(1, 5, 256).is_empty());
    }

    // ── mode extraction (legacy extractMode) ─────────────────────────────

    #[test]
    fn invalid_mode_is_rejected() {
        use emerald_api::proto::blockchain::FeeEstimationMode as M;
        assert_eq!(FeeMode::from_proto(M::Invalid as i32), None);
        assert_eq!(FeeMode::from_proto(9999), None);
    }

    #[test]
    fn known_modes_map() {
        use emerald_api::proto::blockchain::FeeEstimationMode as M;
        assert_eq!(FeeMode::from_proto(M::AvgLast as i32), Some(FeeMode::AvgLast));
        assert_eq!(
            FeeMode::from_proto(M::MinAlways as i32),
            Some(FeeMode::MinAlways)
        );
        assert_eq!(FeeMode::from_proto(M::AvgTop as i32), Some(FeeMode::AvgTop));
    }
}
