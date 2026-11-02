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

//! Per-upstream request limits (the `limits` config of an upstream).
//!
//! Each configured limit is a token bucket, and a call goes to the upstream
//! only when all of them allow it. Otherwise the call is refused locally with
//! [`UpstreamError::LimitReached`], and the router moves on to the next
//! upstream without counting the refusal as an attempt, since nothing was
//! sent.
//!
//! Only client calls are limited. Validation probes and head polling go to
//! the transport below this layer: refusing them would mark a healthy
//! upstream as failed, or freeze its head, just because clients used up its
//! budget.

mod bucket;
mod budget;

pub use budget::RequestBudget;

use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
use crate::upstream::availability::UpstreamAvailability;
use crate::upstream::head::Head;
use crate::upstream::id::UpstreamId;
use crate::upstream::state::UpstreamState;
use crate::upstream::traits::{RpcUpstream, UpstreamError};
use std::sync::Arc;

/// Lets a call through to the wrapped upstream only when the upstream's
/// [`RequestBudget`] allows it.
///
/// Sits below the cache and the method filter, so answers served from the
/// cache and calls refused for their method don't spend the budget.
pub struct RequestLimitGuard {
    inner: Arc<dyn RpcUpstream>,
    budget: Arc<RequestBudget>,
}

impl RequestLimitGuard {
    fn new(inner: Arc<dyn RpcUpstream>, budget: Arc<RequestBudget>) -> Self {
        Self { inner, budget }
    }

    /// `inner` guarded by `budget`, or `inner` as is for an upstream
    /// configured without limits.
    pub fn wrap(
        inner: Arc<dyn RpcUpstream>,
        budget: Option<&Arc<RequestBudget>>,
    ) -> Arc<dyn RpcUpstream> {
        match budget {
            Some(budget) => Arc::new(Self::new(inner, Arc::clone(budget))),
            None => inner,
        }
    }
}

#[async_trait::async_trait]
impl RpcUpstream for RequestLimitGuard {
    async fn call(&self, request: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
        if !self.budget.try_acquire() {
            tracing::debug!(
                "Upstream {}: request limit reached, not calling {}",
                self.inner.id(),
                request.method
            );
            return Err(UpstreamError::LimitReached);
        }
        self.inner.call(request).await
    }

    fn id(&self) -> &UpstreamId {
        self.inner.id()
    }

    fn availability(&self) -> UpstreamAvailability {
        self.inner.availability()
    }

    fn head(&self) -> &dyn Head {
        self.inner.head()
    }

    fn lag(&self) -> Option<u64> {
        self.inner.lag()
    }

    fn state(&self) -> &Arc<UpstreamState> {
        self.inner.state()
    }

    fn allows_method(&self, method: &RpcMethod) -> bool {
        self.inner.allows_method(method)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::limits::{LimitPeriod, RequestLimit};
    use crate::upstream::head::NoHead;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct CountingUpstream {
        calls: AtomicU32,
        state: Arc<UpstreamState>,
    }

    impl CountingUpstream {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicU32::new(0),
                state: Arc::new(UpstreamState::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for CountingUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#).unwrap())
        }
        fn id(&self) -> &UpstreamId {
            crate::upstream::id::stub_id()
        }
        fn availability(&self) -> UpstreamAvailability {
            self.state.availability()
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            None
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
    }

    fn budget(requests: u32) -> Arc<RequestBudget> {
        RequestBudget::new(&[RequestLimit {
            period: LimitPeriod::Minute,
            requests: NonZeroU32::new(requests).unwrap(),
        }])
        .unwrap()
    }

    fn request() -> JsonRpcRequest {
        JsonRpcRequest::new(1, "eth_blockNumber".into(), serde_json::json!([]))
    }

    #[tokio::test(start_paused = true)]
    async fn refuses_without_calling_once_budget_is_spent() {
        let inner = CountingUpstream::new();
        let guard = RequestLimitGuard::new(Arc::clone(&inner) as Arc<dyn RpcUpstream>, budget(2));

        assert!(guard.call(&request()).await.is_ok());
        assert!(guard.call(&request()).await.is_ok());
        let err = guard.call(&request()).await.unwrap_err();

        assert!(matches!(err, UpstreamError::LimitReached));
        assert_eq!(inner.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn refusal_leaves_upstream_available() {
        // Running out of budget is about this moment's traffic, not the
        // upstream's health; it must not drop out of selection.
        let inner = CountingUpstream::new();
        let guard = RequestLimitGuard::new(inner, budget(1));
        guard.call(&request()).await.unwrap();

        assert!(guard.call(&request()).await.is_err());
        assert_eq!(guard.availability(), UpstreamAvailability::Ok);
    }

    #[tokio::test(start_paused = true)]
    async fn guards_sharing_a_budget_share_the_limit() {
        // A remote Dshackle gets one guard per chain, all on one budget.
        let shared = budget(2);
        let a = RequestLimitGuard::new(CountingUpstream::new(), Arc::clone(&shared));
        let b = RequestLimitGuard::new(CountingUpstream::new(), shared);

        assert!(a.call(&request()).await.is_ok());
        assert!(b.call(&request()).await.is_ok());
        assert!(a.call(&request()).await.is_err());
        assert!(b.call(&request()).await.is_err());
    }
}
