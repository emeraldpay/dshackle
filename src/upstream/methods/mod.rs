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
//! Chain-specific method configurations are in [`ethereum`] and [`bitcoin`].

pub mod bitcoin;
pub mod config;
pub mod ethereum;
mod filter;
mod hardcoded;
mod layered;

pub use config::ConfiguredMethods;
pub use filter::MethodFilter;
pub use hardcoded::HardcodedMethods;
pub use layered::LayeredMethods;

use crate::jsonrpc::RpcMethod;
use crate::upstream::quorum::{AlwaysQuorum, CallQuorum, QuorumFactory};
use std::sync::Arc;

/// Default chain-agnostic methods config used for chains without a
/// specialized mapping (e.g. Dshackle remotes that already apply quorum
/// on their side). Always returns [`AlwaysQuorum`] and claims every method
/// as callable so calls pass through.
pub struct DefaultMethods;

impl QuorumFactory for DefaultMethods {
    fn quorum_for(&self, _method: &RpcMethod) -> Box<dyn CallQuorum> {
        Box::new(AlwaysQuorum::new())
    }
}

/// Aggregates multiple per-upstream method configs into one. Mirrors the
/// legacy `AggregatedCallMethods`: a method is considered callable/hardcoded
/// if *any* delegate claims it, and the quorum is taken from the first
/// delegate that supports it. Used as the chain-level [`QuorumFactory`]
/// installed on a [`Multistream`](crate::upstream::Multistream).
pub struct AggregatedMethods {
    delegates: Vec<Arc<dyn QuorumFactory>>,
}

impl AggregatedMethods {
    pub fn new(delegates: Vec<Arc<dyn QuorumFactory>>) -> Self {
        Self { delegates }
    }
}

impl QuorumFactory for AggregatedMethods {
    fn quorum_for(&self, method: &RpcMethod) -> Box<dyn CallQuorum> {
        for d in &self.delegates {
            if d.is_callable(method) || d.is_hardcoded(method) {
                return d.quorum_for(method);
            }
        }
        // No delegate claims the method — fall back to a permissive quorum
        // so the router can still attempt it and surface a real upstream
        // rejection instead of a synthetic one.
        Box::new(AlwaysQuorum::new())
    }

    fn is_callable(&self, method: &RpcMethod) -> bool {
        self.delegates.iter().any(|d| d.is_callable(method))
    }

    fn is_hardcoded(&self, method: &RpcMethod) -> bool {
        self.delegates.iter().any(|d| d.is_hardcoded(method))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::quorum::SelectorHint;

    #[test]
    fn default_methods_always_available() {
        match DefaultMethods.quorum_for(&"anything".into()).selector() {
            SelectorHint::Available => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── AggregatedMethods ────────────────────────────────────────────────

    use crate::upstream::quorum::{NonEmptyQuorum, NotLaggingQuorum};
    use std::collections::HashSet;

    struct FakeMethods {
        callable: HashSet<RpcMethod>,
        hardcoded: HashSet<RpcMethod>,
        quorum: Box<dyn Fn() -> Box<dyn CallQuorum> + Send + Sync>,
    }

    impl QuorumFactory for FakeMethods {
        fn quorum_for(&self, _m: &RpcMethod) -> Box<dyn CallQuorum> {
            (self.quorum)()
        }
        fn is_callable(&self, m: &RpcMethod) -> bool {
            self.callable.contains(m)
        }
        fn is_hardcoded(&self, m: &RpcMethod) -> bool {
            self.hardcoded.contains(m)
        }
    }

    #[test]
    fn aggregated_uses_first_delegate_supporting_method() {
        let a: Arc<dyn QuorumFactory> = Arc::new(FakeMethods {
            callable: HashSet::from(["eth_call".into()]),
            hardcoded: HashSet::new(),
            quorum: Box::new(|| Box::new(NotLaggingQuorum::new(4))),
        });
        let b: Arc<dyn QuorumFactory> = Arc::new(FakeMethods {
            callable: HashSet::from(["eth_sendRawTransaction".into()]),
            hardcoded: HashSet::new(),
            quorum: Box::new(|| Box::new(NonEmptyQuorum::new())),
        });
        let agg = AggregatedMethods::new(vec![a, b]);

        // First delegate matches — its quorum (NotLagging{4}) wins.
        match agg.quorum_for(&"eth_call".into()).selector() {
            SelectorHint::NotLagging { max_lag: 4 } => {}
            other => panic!("unexpected: {other:?}"),
        }
        // Only second delegate claims this method — its quorum is used.
        let q = agg.quorum_for(&"eth_sendRawTransaction".into());
        assert!(matches!(q.selector(), SelectorHint::Available));
    }

    #[test]
    fn aggregated_falls_back_to_always_for_unknown_method() {
        let d: Arc<dyn QuorumFactory> = Arc::new(FakeMethods {
            callable: HashSet::new(),
            hardcoded: HashSet::new(),
            quorum: Box::new(|| Box::new(NotLaggingQuorum::new(0))),
        });
        let agg = AggregatedMethods::new(vec![d]);
        match agg.quorum_for(&"unknown".into()).selector() {
            SelectorHint::Available => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn aggregated_unions_capability_predicates() {
        let a: Arc<dyn QuorumFactory> = Arc::new(FakeMethods {
            callable: HashSet::from(["m1".into()]),
            hardcoded: HashSet::new(),
            quorum: Box::new(|| Box::new(AlwaysQuorum::new())),
        });
        let b: Arc<dyn QuorumFactory> = Arc::new(FakeMethods {
            callable: HashSet::new(),
            hardcoded: HashSet::from(["m2".into()]),
            quorum: Box::new(|| Box::new(AlwaysQuorum::new())),
        });
        let agg = AggregatedMethods::new(vec![a, b]);
        assert!(agg.is_callable(&"m1".into()));
        assert!(!agg.is_callable(&"m2".into()));
        assert!(agg.is_hardcoded(&"m2".into()));
        assert!(!agg.is_hardcoded(&"m1".into()));
    }
}
