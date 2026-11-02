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

//! Per-upstream request limits (the `limits` list of an upstream entry):
//!
//! ```yaml
//! limits:
//!   - period: minute
//!     requests: 10
//!   - period: hour
//!     requests: 300
//! ```

use serde::Deserialize;
use std::num::NonZeroU32;
use std::time::Duration;

/// The span of time a [`RequestLimit`] counts requests over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LimitPeriod {
    /// `minute` in the config.
    Minute,
    /// `hour` in the config.
    Hour,
    /// `day` in the config.
    Day,
}

impl LimitPeriod {
    /// The length of the period, which is also how long a fully spent
    /// budget takes to refill.
    pub fn duration(self) -> Duration {
        match self {
            LimitPeriod::Minute => Duration::from_secs(60),
            LimitPeriod::Hour => Duration::from_secs(60 * 60),
            LimitPeriod::Day => Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// A budget of `requests` calls to the upstream per `period`, enforced as a
/// token bucket: the whole budget may go out in a burst, then it refills
/// evenly over the period. Because the refill is continuous, up to twice
/// `requests` may go out within one period (a full burst, then the refill);
/// only the long-run average is held to `requests` per `period`.
///
/// Zero is rejected at parse time: an upstream that may never be called is a
/// config mistake, and `enabled: false` is the way to turn one off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct RequestLimit {
    /// The span of time the budget is for.
    pub period: LimitPeriod,
    /// How many calls the budget allows per `period`.
    #[serde(rename = "requests", alias = "request")]
    pub requests: NonZeroU32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<Vec<RequestLimit>, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn parses_all_periods() {
        let limits = parse(
            "
- period: minute
  requests: 10
- period: hour
  requests: 300
- period: day
  requests: 5000
",
        )
        .unwrap();

        assert_eq!(
            limits,
            vec![
                RequestLimit {
                    period: LimitPeriod::Minute,
                    requests: NonZeroU32::new(10).unwrap(),
                },
                RequestLimit {
                    period: LimitPeriod::Hour,
                    requests: NonZeroU32::new(300).unwrap(),
                },
                RequestLimit {
                    period: LimitPeriod::Day,
                    requests: NonZeroU32::new(5000).unwrap(),
                },
            ]
        );
    }

    #[test]
    fn accepts_singular_request_key() {
        let limits = parse("- period: minute\n  request: 10").unwrap();
        assert_eq!(limits[0].requests.get(), 10);
    }

    #[test]
    fn rejects_zero_requests() {
        assert!(parse("- period: minute\n  requests: 0").is_err());
    }

    #[test]
    fn rejects_unknown_period() {
        let err = parse("- period: week\n  requests: 10").unwrap_err();
        assert!(err.to_string().contains("unknown variant `week`"), "{err}");
    }

    #[test]
    fn rejects_missing_request_count() {
        assert!(parse("- period: minute").is_err());
    }

    #[test]
    fn period_durations() {
        assert_eq!(LimitPeriod::Minute.duration(), Duration::from_secs(60));
        assert_eq!(LimitPeriod::Hour.duration(), Duration::from_secs(3_600));
        assert_eq!(LimitPeriod::Day.duration(), Duration::from_secs(86_400));
    }
}
