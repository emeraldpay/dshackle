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

//! Pending transactions of a whole chain, gathered from all its sources.
//! Ports the legacy `AggregatedPendingTxes`.

use super::expiring_set::ExpiringSet;
use super::{PendingTxSource, PendingTxStream};
use crate::data::TxId;
use crate::upstream::fanout::SharedFanout;
use futures::stream::{SelectAll, select_all};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

/// How long, and how many, announced transactions are remembered to recognize
/// a repeated announcement. Same bounds as the legacy `AggregatedPendingTxes`:
/// the upstreams of a chain learn about a transaction within moments of each
/// other, so a short memory is enough to tell their announcements apart from
/// a transaction that left the mempool and came back.
const REMEMBER_FOR: Duration = Duration::from_secs(30);
const REMEMBER_LIMIT: usize = 10_000;

/// Several sources announcing as one: each pending transaction comes out
/// once, however many of the sources saw it.
///
/// The merge runs once for all listeners, not once per listener. Legacy
/// filtered every listener's own merge through a single shared memory, so
/// with two listeners each transaction reached only whichever asked first.
pub struct AggregatedPendingTxes {
    sources: Vec<Arc<dyn PendingTxSource>>,
    fanout: Arc<SharedFanout<TxId>>,
}

impl AggregatedPendingTxes {
    /// Nothing is subscribed on the sources until the first listener attaches.
    pub fn new(sources: Vec<Arc<dyn PendingTxSource>>) -> Self {
        Self {
            sources,
            fanout: Arc::new(SharedFanout::new()),
        }
    }

    /// The sources of a chain folded into one: `None` when there are none,
    /// the sole source unchanged (it has nobody to repeat after), or their
    /// aggregate.
    pub fn combined(
        mut sources: Vec<Arc<dyn PendingTxSource>>,
    ) -> Option<Arc<dyn PendingTxSource>> {
        match sources.len() {
            0 => None,
            1 => sources.pop(),
            _ => Some(Arc::new(Self::new(sources))),
        }
    }
}

impl PendingTxSource for AggregatedPendingTxes {
    fn subscribe(&self) -> PendingTxStream {
        let receiver = self.fanout.subscribe(|listeners| {
            let merge = Merge {
                announced: select_all(self.sources.iter().map(|source| source.subscribe())),
                fanout: Arc::clone(&self.fanout),
                listeners,
            };
            tokio::spawn(merge.run());
        });
        // A lagged listener just skips ahead; delivery is best-effort.
        Box::pin(BroadcastStream::new(receiver).filter_map(|item| item.ok()))
    }
}

/// The task merging the sources for the listeners. Lives from the first
/// listener to the last; ending it drops the source streams, which lets each
/// source cancel its own upstream subscription.
struct Merge {
    announced: SelectAll<PendingTxStream>,
    fanout: Arc<SharedFanout<TxId>>,
    listeners: broadcast::Sender<TxId>,
}

impl Merge {
    async fn run(mut self) {
        let mut seen = ExpiringSet::new(REMEMBER_FOR, REMEMBER_LIMIT);
        loop {
            tokio::select! {
                Some(tx) = self.announced.next() => {
                    if seen.insert(tx)
                        && self.listeners.send(tx).is_err()
                        && self.fanout.finish_if_deserted(&self.listeners)
                    {
                        return;
                    }
                }
                _ = self.listeners.closed() => {
                    if self.fanout.finish_if_deserted(&self.listeners) {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A source announcing whatever the test sends through it.
    struct ManualSource {
        announcements: broadcast::Sender<TxId>,
    }

    impl ManualSource {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                announcements: broadcast::channel(16).0,
            })
        }

        fn announce(&self, tx: TxId) {
            self.announcements.send(tx).expect("a listening aggregate");
        }

        fn listeners(&self) -> usize {
            self.announcements.receiver_count()
        }
    }

    impl PendingTxSource for ManualSource {
        fn subscribe(&self) -> PendingTxStream {
            Box::pin(
                BroadcastStream::new(self.announcements.subscribe()).filter_map(|item| item.ok()),
            )
        }
    }

    fn tx(n: u8) -> TxId {
        TxId::from_bytes([n; 32])
    }

    async fn next_tx(stream: &mut PendingTxStream) -> TxId {
        tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a pending transaction in time")
            .expect("an open stream")
    }

    #[tokio::test]
    async fn announces_once_what_several_sources_saw() {
        let (a, b) = (ManualSource::new(), ManualSource::new());
        let aggregated = AggregatedPendingTxes::new(vec![a.clone(), b.clone()]);
        let mut listener = aggregated.subscribe();

        a.announce(tx(1));
        b.announce(tx(1));
        b.announce(tx(2));
        a.announce(tx(2));
        a.announce(tx(3));

        assert_eq!(next_tx(&mut listener).await, tx(1));
        assert_eq!(next_tx(&mut listener).await, tx(2));
        assert_eq!(next_tx(&mut listener).await, tx(3));
    }

    #[tokio::test(start_paused = true)]
    async fn announces_again_once_the_first_sighting_is_forgotten() {
        let a = ManualSource::new();
        let aggregated = AggregatedPendingTxes::new(vec![a.clone(), ManualSource::new()]);
        let mut listener = aggregated.subscribe();

        a.announce(tx(1));
        assert_eq!(next_tx(&mut listener).await, tx(1));

        tokio::time::advance(REMEMBER_FOR).await;
        a.announce(tx(1));
        assert_eq!(next_tx(&mut listener).await, tx(1));
    }

    #[tokio::test]
    async fn every_listener_gets_every_transaction() {
        let (a, b) = (ManualSource::new(), ManualSource::new());
        let aggregated = AggregatedPendingTxes::new(vec![a.clone(), b.clone()]);
        let mut first = aggregated.subscribe();
        let mut second = aggregated.subscribe();

        a.announce(tx(1));
        b.announce(tx(2));

        for listener in [&mut first, &mut second] {
            let mut received = vec![next_tx(listener).await, next_tx(listener).await];
            received.sort_by_key(|tx| *tx.as_bytes());
            assert_eq!(received, vec![tx(1), tx(2)]);
        }
        // Both listeners share the one merge, so each source is listened once.
        assert_eq!(a.listeners(), 1);
        assert_eq!(b.listeners(), 1);
    }

    #[tokio::test]
    async fn releases_the_sources_after_last_listener() {
        let (a, b) = (ManualSource::new(), ManualSource::new());
        let aggregated = AggregatedPendingTxes::new(vec![a.clone(), b.clone()]);

        let listener = aggregated.subscribe();
        assert_eq!((a.listeners(), b.listeners()), (1, 1));

        drop(listener);
        tokio::time::timeout(Duration::from_secs(5), async {
            while a.listeners() + b.listeners() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sources released");

        // And a later listener brings them back.
        let _again = aggregated.subscribe();
        assert_eq!((a.listeners(), b.listeners()), (1, 1));
    }

    #[test]
    fn single_source_is_used_as_is() {
        assert!(AggregatedPendingTxes::combined(vec![]).is_none());

        let only: Arc<dyn PendingTxSource> = ManualSource::new();
        let combined = AggregatedPendingTxes::combined(vec![Arc::clone(&only)]).unwrap();
        assert!(Arc::ptr_eq(&only, &combined));
    }
}
