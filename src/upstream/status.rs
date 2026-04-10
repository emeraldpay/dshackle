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

//! Periodic status reporting for upstream groups.
//!
//! Spawns a background task that logs the state of each blockchain's upstreams
//! every 30 seconds, matching the legacy `Multistream.printStatus()` format:
//!
//! ```text
//! State of ETH: height=12345678, status=[OK/2,LAGGING/1], lag=[0, 1, NA], weak=[upstream3]
//! ```
//!
//! On each tick, the reporter also recalculates lag and derived availability
//! for every upstream (see `UpstreamState::update`).

use crate::blockchain::TargetBlockchain;
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::traits::RpcUpstream;
use itertools::Itertools;
use std::sync::Arc;
use std::time::Duration;

const STATUS_INTERVAL: Duration = Duration::from_secs(30);

/// Holds the upstream references for a single blockchain, used for status reporting.
pub struct ChainStatus {
    pub chain: TargetBlockchain,
    pub upstreams: Vec<Arc<dyn RpcUpstream>>,
}

/// A snapshot of one upstream's state at a single point in time.
struct UpstreamSnapshot {
    id: String,
    availability: UpstreamAvailability,
    lag: Option<u64>,
}

/// Recalculate lag and availability for each upstream in the chain, then
/// format the status line.
fn update_and_format(entry: &ChainStatus) -> String {
    // Snapshot heights once to avoid TOCTOU between best-height scan and
    // per-upstream lag computation.
    let heights: Vec<Option<u64>> = entry
        .upstreams
        .iter()
        .map(|u| u.head().current_height())
        .collect();

    let best_height: Option<u64> = heights.iter().copied().flatten().max();

    // Update lag and derived availability on each upstream
    for (u, &h) in entry.upstreams.iter().zip(&heights) {
        match (best_height, h) {
            (Some(best), Some(h)) => {
                let lag = best.saturating_sub(h);
                u.state().update(lag, Some(h));
            }
            _ => {
                u.state().set_unknown();
            }
        }
    }

    // Snapshot the now-updated state for consistent formatting
    let snapshots: Vec<UpstreamSnapshot> = entry
        .upstreams
        .iter()
        .map(|u| UpstreamSnapshot {
            id: u.id().to_string(),
            availability: u.availability(),
            lag: u.lag(),
        })
        .collect();

    format_status(entry.chain, best_height, &snapshots)
}

/// Formats the status line for a single blockchain, matching the legacy output.
fn format_status(
    chain: TargetBlockchain,
    best_height: Option<u64>,
    snapshots: &[UpstreamSnapshot],
) -> String {
    let height: String = best_height
        .map(|h| h.to_string())
        .unwrap_or_else(|| "?".to_string());

    // Status counts grouped by variant, e.g. "OK/2,LAGGING/1"
    let statuses: String = snapshots
        .iter()
        .map(|s| s.availability)
        .sorted()
        .chunk_by(|s| *s)
        .into_iter()
        .map(|(status, group)| format!("{}/{}", status, group.count()))
        .join(",");

    // Per-upstream lag
    let lag: String = snapshots
        .iter()
        .map(|s| match s.lag {
            Some(v) => v.to_string(),
            None => "NA".to_string(),
        })
        .join(", ");

    // IDs of non-OK upstreams
    let weak: String = snapshots
        .iter()
        .filter(|s| s.availability != UpstreamAvailability::Ok)
        .map(|s| s.id.as_str())
        .join(", ");

    format!(
        "State of {}: height={}, status=[{}], lag=[{}], weak=[{}]",
        chain, height, statuses, lag, weak,
    )
}

/// Spawns a background task that logs upstream status for all chains every 30 seconds.
pub fn start_status_reporter(chains: Vec<ChainStatus>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(STATUS_INTERVAL);
        loop {
            interval.tick().await;
            for entry in &chains {
                tracing::info!("{}", update_and_format(entry));
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::head::{CurrentHeight, Head};
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::UpstreamError;
    use emerald_api::proto::common::ChainRef;

    struct StubUpstream {
        name: String,
        head: Arc<CurrentHeight>,
        state: Arc<UpstreamState>,
    }

    impl StubUpstream {
        fn new(name: &str, height: Option<u64>) -> Arc<dyn RpcUpstream> {
            let head = Arc::new(CurrentHeight::new());
            if let Some(h) = height {
                head.update(h);
            }
            Arc::new(Self {
                name: name.to_string(),
                head,
                state: Arc::new(UpstreamState::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            unimplemented!()
        }
        fn id(&self) -> &str { &self.name }
        fn availability(&self) -> UpstreamAvailability { self.state.availability() }
        fn head(&self) -> &dyn Head { self.head.as_ref() }
        fn lag(&self) -> Option<u64> { self.state.lag() }
        fn state(&self) -> &Arc<UpstreamState> { &self.state }
    }

    #[test]
    fn all_at_same_height_are_ok() {
        let entry = ChainStatus {
            chain: ChainRef::ChainEthereum.into(),
            upstreams: vec![
                StubUpstream::new("infura", Some(100)),
                StubUpstream::new("alchemy", Some(100)),
            ],
        };

        let line = update_and_format(&entry);
        assert_eq!(
            line,
            "State of ETH: height=100, status=[OK/2], lag=[0, 0], weak=[]"
        );
    }

    #[test]
    fn upstream_2_blocks_behind_is_lagging() {
        let entry = ChainStatus {
            chain: ChainRef::ChainEthereum.into(),
            upstreams: vec![
                StubUpstream::new("infura", Some(100)),
                StubUpstream::new("alchemy", Some(98)),
            ],
        };

        let line = update_and_format(&entry);
        assert_eq!(
            line,
            "State of ETH: height=100, status=[OK/1,LAGGING/1], lag=[0, 2], weak=[alchemy]"
        );
    }

    #[test]
    fn upstream_7_blocks_behind_is_syncing() {
        let entry = ChainStatus {
            chain: ChainRef::ChainEthereum.into(),
            upstreams: vec![
                StubUpstream::new("infura", Some(100)),
                StubUpstream::new("alchemy", Some(93)),
            ],
        };

        let line = update_and_format(&entry);
        assert_eq!(
            line,
            "State of ETH: height=100, status=[OK/1,SYNCING/1], lag=[0, 7], weak=[alchemy]"
        );
    }

    #[test]
    fn upstream_at_height_zero_is_syncing() {
        let entry = ChainStatus {
            chain: ChainRef::ChainEthereum.into(),
            upstreams: vec![
                StubUpstream::new("infura", Some(100)),
                StubUpstream::new("alchemy", Some(0)),
            ],
        };

        let line = update_and_format(&entry);
        assert!(line.contains("SYNCING"));
        assert!(line.contains("weak=[alchemy]"));
    }

    #[test]
    fn unknown_height_shows_na_lag() {
        let entry = ChainStatus {
            chain: ChainRef::ChainBitcoin.into(),
            upstreams: vec![StubUpstream::new("node", None)],
        };

        let line = update_and_format(&entry);
        assert_eq!(
            line,
            "State of BTC: height=?, status=[OK/1], lag=[NA], weak=[]"
        );
    }

    #[test]
    fn mixed_known_and_unknown_heights() {
        let entry = ChainStatus {
            chain: ChainRef::ChainEthereum.into(),
            upstreams: vec![
                StubUpstream::new("infura", Some(100)),
                StubUpstream::new("alchemy", None),
            ],
        };

        let line = update_and_format(&entry);
        assert_eq!(
            line,
            "State of ETH: height=100, status=[OK/2], lag=[0, NA], weak=[]"
        );
    }
}
