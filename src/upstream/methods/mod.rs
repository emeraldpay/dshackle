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

//! RPC method handling: filtering, hardcoded responses, and per-chain method
//! definitions.
//!
//! Provides two [`RpcUpstream`] wrappers that sit between the caller and the
//! actual upstream transport:
//!
//! - [`HardcodedMethods`] — intercepts methods with known static responses
//!   (e.g. `net_version`, `eth_chainId`) without hitting the node.
//! - [`MethodFilter`] — rejects methods not in the allowed set.
//!
//! Chain-specific method configurations are in [`ethereum`].

pub mod ethereum;
mod filter;
mod hardcoded;

pub use filter::MethodFilter;
pub use hardcoded::HardcodedMethods;
