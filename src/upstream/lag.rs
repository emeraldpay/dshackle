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

//! Keeps each upstream's lag behind its chain's best head up to date.
//!
//! Lag is recalculated whenever any upstream of the chain reports a higher
//! head, not on a timer. Routing filters on it (the `NotLagging` quorums), and
//! a value sampled while a new block is still propagating would keep a
//! healthy node out of rotation until the next sample, sending its share of
//! calls to the fallbacks. Following every head change, a node that was a
//! block behind for a moment is back at lag 0 as soon as its own head
//! arrives, like with the legacy `HeadLagObserver`.

use crate::upstream::traits::RpcUpstream;
use std::sync::Arc;
use tokio::sync::Notify;

/// Recalculate the lag, and the availability derived from it, of each
/// upstream of one chain against the best head among them.
pub fn update_lags(upstreams: &[Arc<dyn RpcUpstream>]) {
    // Snapshot heights once, so the best height and each lag come from the
    // same moment.
    let heights: Vec<Option<u64>> = upstreams
        .iter()
        .map(|u| u.head().current_height())
        .collect();
    let best_height: Option<u64> = heights.iter().copied().flatten().max();

    for (u, &height) in upstreams.iter().zip(&heights) {
        match (best_height, height) {
            (Some(best), Some(height)) => {
                u.state().update(best.saturating_sub(height), Some(height))
            }
            _ => u.state().set_unknown(),
        }
    }
}

/// Keep the lag of one chain's upstreams current, recalculating it on every
/// head change of any of them.
pub fn start_lag_tracking(upstreams: Vec<Arc<dyn RpcUpstream>>) {
    let signal = Arc::new(Notify::new());
    for u in &upstreams {
        u.head().notify_growth(&signal);
    }
    tokio::spawn(async move {
        loop {
            update_lags(&upstreams);
            // A single pending wake-up covers any number of head changes
            // since the last pass, as each pass reads all heads anew.
            signal.notified().await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
    use crate::upstream::availability::UpstreamAvailability;
    use crate::upstream::head::{CurrentHead, Head};
    use crate::upstream::id::UpstreamId;
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::UpstreamError;
    use std::time::Duration;

    struct StubUpstream {
        name: UpstreamId,
        head: Arc<CurrentHead>,
        state: Arc<UpstreamState>,
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

    fn upstream(name: &str, height: u64) -> (Arc<dyn RpcUpstream>, Arc<CurrentHead>) {
        let head = Arc::new(CurrentHead::new());
        head.update(height);
        let upstream = Arc::new(StubUpstream {
            name: name.parse().unwrap(),
            head: Arc::clone(&head),
            state: Arc::new(UpstreamState::new()),
        });
        (upstream, head)
    }

    async fn wait_for_lag(upstream: &Arc<dyn RpcUpstream>, lag: u64) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while upstream.lag() != Some(lag) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("lag didn't become {lag}, it's {:?}", upstream.lag()));
    }

    #[test]
    fn lag_is_the_distance_to_the_best_head() {
        let (first, _) = upstream("first", 100);
        let (behind, _) = upstream("behind", 97);

        update_lags(&[Arc::clone(&first), Arc::clone(&behind)]);

        assert_eq!(first.lag(), Some(0));
        assert_eq!(behind.lag(), Some(3));
    }

    #[tokio::test]
    async fn lag_follows_every_head_change() {
        let (fast, fast_head) = upstream("fast", 100);
        let (slow, slow_head) = upstream("slow", 100);
        start_lag_tracking(vec![Arc::clone(&fast), Arc::clone(&slow)]);
        wait_for_lag(&slow, 0).await;

        // A new block reaches one upstream first...
        fast_head.update(101);
        wait_for_lag(&slow, 1).await;
        assert_eq!(slow.availability(), UpstreamAvailability::Ok);

        // ...and the other one catching up clears its lag right away, not on
        // some later sample.
        slow_head.update(101);
        wait_for_lag(&slow, 0).await;
    }
}
