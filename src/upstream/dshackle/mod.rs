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

//! Dshackle gRPC upstream — connects to another Dshackle instance and forwards
//! RPC calls via the `NativeCall` gRPC method.
//!
//! Unlike Ethereum/Bitcoin upstreams where we talk directly to a blockchain
//! node, a Dshackle upstream multiplexes **multiple blockchains** through a
//! single gRPC connection. Available chains are discovered dynamically by
//! calling the remote's `Describe` RPC.

pub mod head;
pub mod status;
pub mod upstream;

pub use upstream::DshackleUpstream;
