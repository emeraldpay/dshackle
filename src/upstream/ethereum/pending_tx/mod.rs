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

//! Hashes of pending (not yet mined) transactions, as the chain's upstreams
//! announce them over `newPendingTransactions`.
//!
//! An upstream that can push them is a [`PendingTxSource`]: a node reached
//! over WebSocket ([`WsPendingTxes`]) or a remote Dshackle relaying the topic
//! ([`RelayedPendingTxes`]). A chain with several of them reads through
//! [`AggregatedPendingTxes`], which announces each transaction once no matter
//! how many upstreams saw it.
//!
//! Every source is demand-driven, like the legacy `SharedFluxHolder` behind
//! `DefaultPendingTxesSource`: nothing is subscribed upstream until someone
//! listens, all listeners share one upstream subscription, and it is cancelled
//! once the last of them is gone — a mempool feed is too heavy to keep
//! streaming for nobody.

mod aggregated;
mod expiring_set;
mod relayed;
mod ws;

pub use aggregated::AggregatedPendingTxes;
pub use relayed::RelayedPendingTxes;
pub use ws::WsPendingTxes;

use crate::data::TxId;
use std::pin::Pin;
use tokio_stream::Stream;

/// A stream of pending transaction hashes.
pub type PendingTxStream = Pin<Box<dyn Stream<Item = TxId> + Send>>;

/// Something that announces pending transactions. Ports the legacy
/// `PendingTxesSource`.
pub trait PendingTxSource: Send + Sync {
    /// Start listening. Delivery is best-effort: a listener that falls too
    /// far behind skips ahead, and whatever is announced while the upstream
    /// connection is being re-established is lost.
    fn subscribe(&self) -> PendingTxStream;
}

/// The hash carried by a `newPendingTransactions` message, which is a JSON
/// string. `None` for anything else — e.g. the full transaction objects some
/// nodes can be asked for.
fn tx_id_from_message(message: &[u8]) -> Option<TxId> {
    serde_json::from_slice::<&str>(message).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_hash_from_json_string() {
        let message = br#""0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75""#;
        assert_eq!(
            tx_id_from_message(message).unwrap().to_hex_prefixed(),
            "0xfc58a258adccc94466ae967b1178eea721349b0667f59d5fe1b0b436460bce75"
        );
    }

    #[test]
    fn rejects_what_is_not_a_hash() {
        assert!(tx_id_from_message(br#""0xfc58""#).is_none());
        assert!(tx_id_from_message(br#"{"hash":"0xfc58"}"#).is_none());
        assert!(tx_id_from_message(b"").is_none());
    }
}
