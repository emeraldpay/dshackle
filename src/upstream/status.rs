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
//! It only reports: the lag it shows is kept current by
//! [`lag`](super::lag) on every head change.

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

/// The status line of one chain, from the current state of its upstreams.
fn status_line(entry: &ChainStatus) -> String {
    let best_height: Option<u64> = entry
        .upstreams
        .iter()
        .filter_map(|u| u.head().current_height())
        .max();

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
                tracing::info!("{}", status_line(entry));
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::head::{CurrentHead, Head};
    use crate::upstream::id::UpstreamId;
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::UpstreamError;
    use emerald_api::proto::common::ChainRef;

    struct StubUpstream {
        name: UpstreamId,
        head: Arc<CurrentHead>,
        state: Arc<UpstreamState>,
    }

    impl StubUpstream {
        fn new(name: &str, height: Option<u64>) -> Arc<dyn RpcUpstream> {
            let head = Arc::new(CurrentHead::new());
            if let Some(h) = height {
                head.update(h);
            }
            Arc::new(Self {
                name: name.parse().unwrap(),
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
        fn id(&self) -> &UpstreamId {
            &self.name
        }
        fn availability(&self) -> UpstreamAvailability {
            self.state.availability()
        }
        fn head(&self) -> &dyn Head {
            self.head.as_ref()
        }
        fn lag(&self) -> Option<u64> {
            self.state.lag()
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
    }

    /// The line as reported once lag tracking has caught up with the heads.
    fn report(entry: &ChainStatus) -> String {
        crate::upstream::lag::update_lags(&entry.upstreams);
        status_line(entry)
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

        let line = report(&entry);
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

        let line = report(&entry);
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

        let line = report(&entry);
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

        let line = report(&entry);
        assert!(line.contains("SYNCING"));
        assert!(line.contains("weak=[alchemy]"));
    }

    #[test]
    fn unknown_height_shows_na_lag() {
        let entry = ChainStatus {
            chain: ChainRef::ChainBitcoin.into(),
            upstreams: vec![StubUpstream::new("node", None)],
        };

        let line = report(&entry);
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

        let line = report(&entry);
        assert_eq!(
            line,
            "State of ETH: height=100, status=[OK/2], lag=[0, NA], weak=[]"
        );
    }
}
