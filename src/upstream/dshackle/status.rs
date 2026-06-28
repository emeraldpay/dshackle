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

//! Status tracking for Dshackle gRPC upstreams via `SubscribeStatus`.
//!
//! Subscribes to the remote Dshackle's `SubscribeStatus` stream and records the
//! availability it reports for our chain into the upstream's [`UpstreamState`],
//! so a remote that declares a chain lagging/syncing/unavailable is taken out
//! of rotation here too. Ports the legacy `GrpcUpstreams.startStatusUpdates` →
//! `DefaultUpstream.onStatus` path.
//!
//! Re-subscribes automatically when the stream closes or errors. A dropped
//! stream leaves the last reported status in place; a genuinely unreachable
//! remote is still caught by lag tracking (its head stops advancing).

use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::state::UpstreamState;
use emerald_api::proto::blockchain::blockchain_client::BlockchainClient;
use emerald_api::proto::blockchain::{AvailabilityEnum, StatusRequest};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

/// Pause between reconnection attempts.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Spawns a background task that subscribes to `SubscribeStatus` on the remote
/// Dshackle for one chain and records the reported availability.
pub fn start_status_subscriber(
    upstream_id: String,
    chain_ref: i32,
    client: BlockchainClient<Channel>,
    state: Arc<UpstreamState>,
) {
    tokio::spawn(async move {
        loop {
            subscribe_once(&upstream_id, chain_ref, &client, &state).await;
            tokio::time::sleep(RETRY_DELAY).await;
        }
    });
}

async fn subscribe_once(
    upstream_id: &str,
    chain_ref: i32,
    client: &BlockchainClient<Channel>,
    state: &UpstreamState,
) {
    // Ask only for our chain; the remote streams a `ChainStatus` per chain.
    let request = StatusRequest {
        chains: vec![chain_ref],
    };
    let mut client = client.clone();

    match client.subscribe_status(request).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            tracing::debug!(upstream = %upstream_id, chain = chain_ref, "listening for status updates");

            loop {
                match stream.message().await {
                    Ok(Some(status)) => {
                        // A remote may ignore the filter and stream every chain;
                        // act only on ours.
                        if status.chain != chain_ref {
                            continue;
                        }
                        let availability = map_availability(status.availability);
                        tracing::trace!(
                            upstream = %upstream_id,
                            %availability,
                            "remote reported status"
                        );
                        state.set_reported(availability);
                    }
                    Ok(None) => {
                        tracing::debug!(
                            upstream = %upstream_id,
                            "status stream closed, will re-subscribe"
                        );
                        break;
                    }
                    Err(e) => {
                        tracing::debug!(
                            upstream = %upstream_id,
                            error = %e,
                            "status stream error, will re-subscribe"
                        );
                        break;
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!(
                upstream = %upstream_id,
                error = %e,
                "subscribe_status failed, retrying"
            );
        }
    }
}

/// Map the remote's `AvailabilityEnum` to our [`UpstreamAvailability`]. Unknown
/// and unavailable both become `Unavailable`, matching the legacy
/// `UpstreamAvailability.fromGrpc` fallback.
fn map_availability(value: i32) -> UpstreamAvailability {
    match AvailabilityEnum::try_from(value) {
        Ok(AvailabilityEnum::AvailOk) => UpstreamAvailability::Ok,
        Ok(AvailabilityEnum::AvailLagging) => UpstreamAvailability::Lagging,
        Ok(AvailabilityEnum::AvailImmature) => UpstreamAvailability::Immature,
        Ok(AvailabilityEnum::AvailSyncing) => UpstreamAvailability::Syncing,
        // AvailUnavailable, AvailUnknown, and any unrecognized value.
        _ => UpstreamAvailability::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_availabilities() {
        assert_eq!(
            map_availability(AvailabilityEnum::AvailOk as i32),
            UpstreamAvailability::Ok
        );
        assert_eq!(
            map_availability(AvailabilityEnum::AvailLagging as i32),
            UpstreamAvailability::Lagging
        );
        assert_eq!(
            map_availability(AvailabilityEnum::AvailImmature as i32),
            UpstreamAvailability::Immature
        );
        assert_eq!(
            map_availability(AvailabilityEnum::AvailSyncing as i32),
            UpstreamAvailability::Syncing
        );
        assert_eq!(
            map_availability(AvailabilityEnum::AvailUnavailable as i32),
            UpstreamAvailability::Unavailable
        );
    }

    #[test]
    fn maps_unknown_and_garbage_to_unavailable() {
        assert_eq!(
            map_availability(AvailabilityEnum::AvailUnknown as i32),
            UpstreamAvailability::Unavailable
        );
        assert_eq!(map_availability(999), UpstreamAvailability::Unavailable);
    }
}
