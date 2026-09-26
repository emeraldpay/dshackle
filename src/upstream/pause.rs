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

//! How long to keep an upstream out of rotation after it refused a call.
//!
//! The pause grows exponentially while the upstream keeps refusing: a node
//! that's briefly at capacity is back in rotation within a second, while one
//! that stays unavailable for hours (e.g. Erigon doing DB maintenance) is
//! probed less and less often instead of taking a share of real requests every
//! few seconds.

use std::time::Duration;

/// Caps the growth, so a recovered upstream is picked up again within this
/// time even after a long outage.
const MAX_PAUSE: Duration = Duration::from_secs(300);

/// Why an upstream is paused, which decides how long the pause lasts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PauseReason {
    /// The node itself says it's at capacity (Erigon "server overloaded").
    /// Usually clears within moments, so the first pause is short.
    Overloaded,
    /// A provider refuses calls with HTTP 429 (or a JSON-RPC error with the
    /// same meaning), or the upstream answers 401/502–504. Provider limits
    /// are counted in windows of seconds to days, so there's no point in
    /// coming back sooner than that.
    RateLimited,
}

impl PauseReason {
    fn initial(&self) -> Duration {
        match self {
            PauseReason::Overloaded => Duration::from_millis(500),
            PauseReason::RateLimited => Duration::from_secs(10),
        }
    }

    /// The pause after `strikes` previous pauses with no successful call in
    /// between: the initial pause, doubled per strike, up to the cap.
    pub fn cooldown(&self, strikes: u32) -> Duration {
        // Beyond 2^16 the product is past any cap anyway; the clamp keeps the
        // shift from overflowing.
        let factor = 1u32 << strikes.min(16);
        self.initial().saturating_mul(factor).min(MAX_PAUSE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overload_starts_short_and_doubles() {
        let reason = PauseReason::Overloaded;
        assert_eq!(reason.cooldown(0), Duration::from_millis(500));
        assert_eq!(reason.cooldown(1), Duration::from_secs(1));
        assert_eq!(reason.cooldown(2), Duration::from_secs(2));
    }

    #[test]
    fn rate_limit_starts_at_ten_seconds() {
        let reason = PauseReason::RateLimited;
        assert_eq!(reason.cooldown(0), Duration::from_secs(10));
        assert_eq!(reason.cooldown(1), Duration::from_secs(20));
    }

    #[test]
    fn growth_is_capped() {
        assert_eq!(
            PauseReason::Overloaded.cooldown(20),
            Duration::from_secs(300)
        );
        assert_eq!(
            PauseReason::RateLimited.cooldown(u32::MAX),
            Duration::from_secs(300)
        );
    }
}
