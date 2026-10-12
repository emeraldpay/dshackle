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

//! Pending transactions relayed from a remote Dshackle. Ports the legacy
//! `DshacklePendingTxesSource`.

use super::{PendingTxSource, PendingTxStream, tx_id_from_message};
use crate::upstream::egress::{EgressSubscription, METHOD_PENDING_TXES};
use std::sync::Arc;
use tokio_stream::StreamExt;

/// The `newPendingTransactions` topic of a subscription relay, read as
/// transaction hashes so it can be aggregated with the chain's other sources.
///
/// The relay already shares one remote stream between its subscribers and
/// drops it when they leave, so this is only a typed view of it.
pub struct RelayedPendingTxes {
    relay: Arc<dyn EgressSubscription>,
}

impl RelayedPendingTxes {
    /// `None` unless the relay advertises the topic: a remote with no
    /// pending-transaction source of its own has nothing to relay.
    pub fn of(relay: Arc<dyn EgressSubscription>) -> Option<Self> {
        relay
            .available_topics()
            .iter()
            .any(|topic| topic == METHOD_PENDING_TXES)
            .then_some(Self { relay })
    }
}

impl PendingTxSource for RelayedPendingTxes {
    fn subscribe(&self) -> PendingTxStream {
        match self.relay.subscribe(METHOD_PENDING_TXES, None) {
            Ok(messages) => Box::pin(messages.filter_map(|message| tx_id_from_message(&message))),
            Err(e) => {
                // `of` only accepts a relay advertising the topic, so this is
                // a relay breaking its own promise; say so instead of
                // leaving the listener wondering about the silence.
                tracing::warn!(error = ?e, "pending transactions are not relayed as advertised");
                Box::pin(tokio_stream::empty())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::egress::{EgressError, SubscriptionStream};

    /// An egress serving one topic with a fixed set of messages.
    struct FixedEgress {
        topic: &'static str,
        messages: Vec<&'static str>,
    }

    impl EgressSubscription for FixedEgress {
        fn subscribe(
            &self,
            method: &str,
            _params: Option<serde_json::Value>,
        ) -> Result<SubscriptionStream, EgressError> {
            if method != self.topic {
                return Err(EgressError::UnsupportedMethod(method.to_string()));
            }
            let messages: Vec<Vec<u8>> = self
                .messages
                .iter()
                .map(|message| message.as_bytes().to_vec())
                .collect();
            Ok(Box::pin(tokio_stream::iter(messages)))
        }

        fn available_topics(&self) -> Vec<String> {
            vec![self.topic.to_string()]
        }
    }

    #[tokio::test]
    async fn reads_relayed_messages_as_hashes() {
        let source = RelayedPendingTxes::of(Arc::new(FixedEgress {
            topic: "newPendingTransactions",
            messages: vec![
                r#""0x1111111111111111111111111111111111111111111111111111111111111111""#,
                "not a hash",
                r#""0x2222222222222222222222222222222222222222222222222222222222222222""#,
            ],
        }))
        .unwrap();

        let received: Vec<String> = source
            .subscribe()
            .map(|tx| tx.to_hex_prefixed())
            .collect()
            .await;
        assert_eq!(
            received,
            vec![
                "0x1111111111111111111111111111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222222222222222222222222222",
            ]
        );
    }

    #[test]
    fn needs_a_relay_advertising_the_topic() {
        let relay = Arc::new(FixedEgress {
            topic: "newHeads",
            messages: vec![],
        });
        assert!(RelayedPendingTxes::of(relay).is_none());
    }
}
