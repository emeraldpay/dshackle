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

use crate::config::limits::RequestLimit;
use tokio::time::Instant;

/// A token bucket for one [`RequestLimit`]: holds up to `requests` tokens,
/// one spent per call, and refills continuously at `requests` per `period`.
///
/// A full bucket allows a burst of the whole period's budget at once, after
/// which calls are paced at the refill rate. Over any span of one `period`,
/// at most twice the budget gets through (a full bucket plus a full refill),
/// and over long spans the average converges to the configured rate.
///
/// The level is kept in integer units where one token is `period` worth of
/// nanoseconds and each elapsed nanosecond adds `requests` units. That makes
/// the refill exact: a float level would drift, and a bucket could come up a
/// rounding error short of a token at the very moment one is due.
pub(super) struct TokenBucket {
    /// Units per token: the period in nanoseconds.
    token: u128,
    /// Units added per elapsed nanosecond: the request count.
    refill_rate: u128,
    capacity: u128,
    level: u128,
    refilled_at: Instant,
}

impl TokenBucket {
    /// A full bucket, so a freshly started upstream may take its whole budget.
    pub fn new(limit: &RequestLimit, now: Instant) -> Self {
        let token = limit.period.duration().as_nanos();
        let refill_rate = u128::from(limit.requests.get());
        let capacity = token * refill_rate;
        Self {
            token,
            refill_rate,
            capacity,
            level: capacity,
            refilled_at: now,
        }
    }

    pub fn refill(&mut self, now: Instant) {
        // `saturating_duration_since` because callers on different threads
        // read the clock before taking the budget lock, so a later caller can
        // arrive here with an earlier `now`.
        let elapsed = now.saturating_duration_since(self.refilled_at).as_nanos();
        if elapsed == 0 {
            return;
        }
        self.level = self
            .level
            .saturating_add(elapsed.saturating_mul(self.refill_rate))
            .min(self.capacity);
        self.refilled_at = now;
    }

    pub fn has_token(&self) -> bool {
        self.level >= self.token
    }

    /// Spends one token. The caller checks [`has_token`](Self::has_token)
    /// first, which is how several buckets get spent all together or not at
    /// all.
    pub fn take(&mut self) {
        self.level -= self.token;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::limits::LimitPeriod;
    use std::num::NonZeroU32;
    use std::time::Duration;

    fn bucket(period: LimitPeriod, requests: u32, now: Instant) -> TokenBucket {
        TokenBucket::new(
            &RequestLimit {
                period,
                requests: NonZeroU32::new(requests).unwrap(),
            },
            now,
        )
    }

    fn drain(bucket: &mut TokenBucket) -> u32 {
        let mut taken = 0;
        while bucket.has_token() {
            bucket.take();
            taken += 1;
        }
        taken
    }

    #[test]
    fn starts_full() {
        let now = Instant::now();
        let mut b = bucket(LimitPeriod::Minute, 10, now);

        assert_eq!(drain(&mut b), 10);
    }

    #[test]
    fn refills_one_token_per_interval() {
        let start = Instant::now();
        let mut b = bucket(LimitPeriod::Minute, 10, start);
        drain(&mut b);

        b.refill(start + Duration::from_millis(5_999));
        assert!(!b.has_token());

        b.refill(start + Duration::from_secs(6));
        assert_eq!(drain(&mut b), 1);
    }

    #[test]
    fn refill_accumulates_across_calls() {
        let start = Instant::now();
        let mut b = bucket(LimitPeriod::Minute, 10, start);
        drain(&mut b);

        // Two half-intervals add up to one token; nothing is lost between.
        b.refill(start + Duration::from_secs(3));
        assert!(!b.has_token());
        b.refill(start + Duration::from_secs(6));
        assert!(b.has_token());
    }

    #[test]
    fn refill_stops_at_capacity() {
        let start = Instant::now();
        let mut b = bucket(LimitPeriod::Hour, 300, start);
        drain(&mut b);

        b.refill(start + Duration::from_secs(10 * 3_600));
        assert_eq!(drain(&mut b), 300);
    }

    #[test]
    fn earlier_time_does_not_refill_or_rewind() {
        let start = Instant::now();
        let mut b = bucket(LimitPeriod::Minute, 1, start + Duration::from_secs(1));
        b.take();

        b.refill(start);
        assert!(!b.has_token());

        b.refill(start + Duration::from_secs(61));
        assert!(b.has_token());
    }

    #[test]
    fn large_day_budget_does_not_overflow() {
        let start = Instant::now();
        let mut b = bucket(LimitPeriod::Day, u32::MAX, start);
        b.take();

        b.refill(start + Duration::from_secs(365 * 86_400));
        assert!(b.has_token());
    }
}
