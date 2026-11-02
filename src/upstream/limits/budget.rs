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

use super::bucket::TokenBucket;
use crate::config::limits::RequestLimit;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::time::Instant;

/// The request budget of one upstream: one [`TokenBucket`] per configured
/// limit, and a call may go out only when every bucket has a token.
///
/// Shared by everything that sends client calls to the upstream, e.g. each
/// chain a remote Dshackle serves, since the configured limit is for the
/// upstream as a whole.
pub struct RequestBudget {
    buckets: Mutex<Vec<TokenBucket>>,
}

impl RequestBudget {
    /// The budget for `limits`, ready to share between guards, or `None`
    /// when there are none to enforce.
    pub fn new(limits: &[RequestLimit]) -> Option<Arc<Self>> {
        if limits.is_empty() {
            return None;
        }
        let now = Instant::now();
        Some(Arc::new(Self {
            buckets: Mutex::new(limits.iter().map(|l| TokenBucket::new(l, now)).collect()),
        }))
    }

    /// Spends a token from every bucket if all of them have one, otherwise
    /// spends nothing and returns `false`.
    ///
    /// All-or-nothing matters with several limits: a call the daily limit
    /// refuses never reaches the upstream, so it must not use up the minute
    /// and hour budgets either.
    pub fn try_acquire(&self) -> bool {
        let now = Instant::now();
        // The guarded section is a few integer operations and never panics
        // midway, so a poisoned lock still holds consistent buckets.
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        for bucket in buckets.iter_mut() {
            bucket.refill(now);
        }
        if !buckets.iter().all(TokenBucket::has_token) {
            return false;
        }
        for bucket in buckets.iter_mut() {
            bucket.take();
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::limits::LimitPeriod;
    use std::num::NonZeroU32;
    use std::time::Duration;

    fn limit(period: LimitPeriod, requests: u32) -> RequestLimit {
        RequestLimit {
            period,
            requests: NonZeroU32::new(requests).unwrap(),
        }
    }

    fn acquired(budget: &RequestBudget, attempts: u32) -> u32 {
        (0..attempts).filter(|_| budget.try_acquire()).count() as u32
    }

    #[test]
    fn no_limits_means_no_budget() {
        assert!(RequestBudget::new(&[]).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn single_limit_allows_its_budget() {
        let budget = RequestBudget::new(&[limit(LimitPeriod::Minute, 10)]).unwrap();

        assert_eq!(acquired(&budget, 15), 10);
    }

    #[tokio::test(start_paused = true)]
    async fn strictest_limit_wins() {
        let budget =
            RequestBudget::new(&[limit(LimitPeriod::Minute, 10), limit(LimitPeriod::Hour, 3)])
                .unwrap();

        assert_eq!(acquired(&budget, 10), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn refused_call_spends_nothing() {
        let budget =
            RequestBudget::new(&[limit(LimitPeriod::Minute, 2), limit(LimitPeriod::Hour, 4)])
                .unwrap();
        assert_eq!(acquired(&budget, 5), 2);

        // A minute later the minute bucket has refilled. The hour bucket
        // must still hold the 2 tokens the refusals above didn't take.
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(acquired(&budget, 5), 2);

        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(acquired(&budget, 5), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn recovers_as_time_passes() {
        let budget = RequestBudget::new(&[limit(LimitPeriod::Minute, 10)]).unwrap();
        acquired(&budget, 10);

        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(acquired(&budget, 10), 5);
    }
}
