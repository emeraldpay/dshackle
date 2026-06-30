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

//! ERC-20 allowance tracking for the gRPC `GetAddressAllowance` /
//! `SubscribeAddressAllowance` methods.
//!
//! Like the legacy `TrackERC20Allowance`, this is a forwarding proxy rather than
//! a local implementation: the edge instance doesn't scan `Approval` logs or
//! call `allowance()` itself — it forwards the request to a Dshackle gRPC
//! upstream that advertises the `ALLOWANCE` capability and relays its stream.
//! Without such an upstream the chain reports `UNAVAILABLE`, exactly as legacy
//! does. The dispatch lives on [`UpstreamManager`](super::UpstreamManager); this
//! module just holds the shared stream type and error.

use emerald_api::proto::blockchain::AddressAllowance;
use std::pin::Pin;
use tokio_stream::Stream;

/// A stream of `AddressAllowance` responses relayed from the remote upstream.
pub type AllowanceStream =
    Pin<Box<dyn Stream<Item = Result<AddressAllowance, tonic::Status>> + Send>>;

/// Why an allowance request can't be served locally.
#[derive(Debug)]
pub enum AllowanceError {
    /// No upstream is configured for the requested chain, or the chain id is
    /// unknown.
    Unavailable(i32),
    /// No upstream on the chain advertises the `ALLOWANCE` capability, so there
    /// is nothing to forward to (legacy `UnsupportedBlockchain`).
    Unsupported(i32),
    /// The remote upstream rejected or failed the forwarded call.
    Remote(tonic::Status),
}

impl AllowanceError {
    /// Map to the gRPC status the handler returns.
    pub fn into_status(self) -> tonic::Status {
        match self {
            AllowanceError::Unavailable(chain) | AllowanceError::Unsupported(chain) => {
                tonic::Status::unavailable(format!("BLOCKCHAIN UNAVAILABLE: {chain}"))
            }
            // Surface the remote's own status verbatim.
            AllowanceError::Remote(status) => status,
        }
    }
}
