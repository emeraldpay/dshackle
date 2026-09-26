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

//! Prepares a call for routing: the final form of the request, and what an
//! upstream must have to answer it.
//!
//! It runs once per call, on the chain level, before any upstream is picked
//! (the counterpart of the legacy `NormalizingReader`, which sat in front of
//! the chain's upstreams the same way). Routing then decides from the plan
//! whether an upstream's lag behind the chain head matters at all: most calls
//! read data at a fixed, often old, block, which any node that has that block
//! answers the same, however far it's behind the head.

use crate::jsonrpc::JsonRpcRequest;
use std::borrow::Cow;

/// A call as it's going to be routed.
pub struct CallPlan<'a> {
    /// The request to send: the client's own, or an equivalent that names
    /// its block by something more definite (a tag resolved to a height, a
    /// height to a hash). A rewrite serves the same data, but routes and
    /// caches better: a block hash is immutable and answered from the cache.
    pub request: Cow<'a, JsonRpcRequest>,
    /// The block the call reads, if it names one.
    pub block: Option<BlockRead>,
}

impl<'a> CallPlan<'a> {
    /// The client's request unchanged, with no block to consider.
    pub fn as_is(request: &'a JsonRpcRequest) -> Self {
        Self {
            request: Cow::Borrowed(request),
            block: None,
        }
    }

    /// Whether the request to send is a rewrite of the client's. A rewrite
    /// relies on what's known right now (a cached height→hash that a reorg can
    /// replace), so when it gets no answer the client's request decides.
    pub fn is_rewrite(&self) -> bool {
        matches!(self.request, Cow::Owned(_))
    }
}

/// The block a call reads, which decides what an upstream needs to answer it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockRead {
    /// State at the block (`eth_call`, `eth_getBalance` at a height). A node
    /// that doesn't have the block answers with wrong data (an empty balance,
    /// say) rather than an error, so only upstreams that have reached it can
    /// take the call, and it fails when none has.
    State(u64),
    /// The block itself, or what's in it (`eth_getBlockByNumber`). A node
    /// without the block answers `null`, which is honest, so it only narrows
    /// down how far behind an upstream may be — counted from this block, not
    /// the chain head.
    Block(u64),
}

/// Chain-specific rules for preparing a call.
pub trait CallPlanner: Send + Sync {
    /// Prepare `request`. `head` gives the chain's current head height; it
    /// scans every upstream, so it's called only for a call that needs it.
    fn plan<'a>(&self, request: &'a JsonRpcRequest, head: &dyn Fn() -> Option<u64>)
    -> CallPlan<'a>;
}

/// For chains whose calls have nothing to prepare (Bitcoin; legacy's Bitcoin
/// `NormalizingReader` passed everything through as well).
pub struct AsIs;

impl CallPlanner for AsIs {
    fn plan<'a>(
        &self,
        request: &'a JsonRpcRequest,
        _head: &dyn Fn() -> Option<u64>,
    ) -> CallPlan<'a> {
        CallPlan::as_is(request)
    }
}
