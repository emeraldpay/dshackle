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

//! One shared upstream stream fanned out to any number of local subscribers.
//!
//! Demand-driven, like the legacy `SharedFluxHolder`: the first subscriber
//! starts the producer, everyone shares one broadcast channel, and the
//! producer stops once the last subscriber is gone. Backs the Bitcoin ZMQ
//! topics and the remote-Dshackle subscription relays, which would otherwise
//! each keep an upstream connection streaming for nobody.

use std::sync::Mutex;
use tokio::sync::broadcast;

/// How many messages a subscriber may fall behind before it starts losing
/// them. Delivery is best-effort, matching the legacy `directBestEffort`
/// sink: a slow consumer skips ahead rather than back-pressuring the
/// upstream connection.
const CHANNEL_CAPACITY: usize = 256;

/// The demand-driven broadcast side of one shared upstream stream.
///
/// Holds no channel until someone subscribes; the owner's producer task is
/// started by the first [`subscribe`](Self::subscribe) and is expected to
/// stop when [`finish_if_deserted`](Self::finish_if_deserted) says so.
pub struct SharedFanout<T> {
    /// The live producer's broadcast side, present only while it runs.
    sender: Mutex<Option<broadcast::Sender<T>>>,
}

impl<T: Clone> SharedFanout<T> {
    /// An idle fan-out: no channel exists until the first subscriber attaches.
    pub fn new() -> Self {
        Self {
            sender: Mutex::new(None),
        }
    }

    /// Attach to the shared stream. When this is the first subscriber,
    /// `start` receives the broadcast side so the caller can spawn its
    /// producer task.
    pub fn subscribe(&self, start: impl FnOnce(broadcast::Sender<T>)) -> broadcast::Receiver<T> {
        let mut sender = self.sender.lock().expect("fanout lock poisoned");
        if let Some(tx) = sender.as_ref() {
            return tx.subscribe();
        }
        let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
        *sender = Some(tx.clone());
        start(tx);
        rx
    }

    /// Whether the producer should stop: no subscribers remain. Checked under
    /// the lock because a new subscriber may be attaching to this very sender
    /// at the same moment — stopping then would hand it a stream that is dead
    /// on arrival. On `true` the slot is cleared, so the next subscriber
    /// starts a fresh producer.
    pub fn finish_if_deserted(&self, tx: &broadcast::Sender<T>) -> bool {
        let mut sender = self.sender.lock().expect("fanout lock poisoned");
        if tx.receiver_count() > 0 {
            return false;
        }
        *sender = None;
        true
    }

    /// Whether a producer currently runs — the slot holds a live channel. It
    /// stays `true` between the last subscriber leaving and the producer
    /// noticing via [`finish_if_deserted`](Self::finish_if_deserted).
    pub(crate) fn is_live(&self) -> bool {
        self.sender.lock().expect("fanout lock poisoned").is_some()
    }

    /// Number of currently attached subscribers.
    pub(crate) fn subscribers(&self) -> usize {
        self.sender
            .lock()
            .expect("fanout lock poisoned")
            .as_ref()
            .map(|tx| tx.receiver_count())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_producer_once_and_shares_the_channel() {
        let fanout: SharedFanout<u8> = SharedFanout::new();
        let mut starts = 0;
        let first = fanout.subscribe(|_| starts += 1);
        let second = fanout.subscribe(|_| starts += 1);
        assert_eq!(starts, 1);
        assert_eq!(fanout.subscribers(), 2);
        drop(first);
        drop(second);
    }

    #[test]
    fn deserted_check_clears_the_slot_for_a_restart() {
        let fanout: SharedFanout<u8> = SharedFanout::new();
        let mut side = None;
        let rx = fanout.subscribe(|tx| side = Some(tx));
        let tx = side.unwrap();

        // A live subscriber keeps the producer running.
        assert!(!fanout.finish_if_deserted(&tx));

        drop(rx);
        assert!(fanout.finish_if_deserted(&tx));

        // The slot is cleared: the next subscriber starts a fresh producer.
        let mut restarted = false;
        let _rx = fanout.subscribe(|_| restarted = true);
        assert!(restarted);
    }
}
