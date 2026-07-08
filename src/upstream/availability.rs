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

//! Upstream availability status, used for routing decisions and status reporting.

use std::fmt;

/// Availability status of an upstream node, ordered from best to worst.
///
/// The ordering matches the legacy gRPC IDs and is used to determine whether
/// an upstream is healthy enough for request routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UpstreamAvailability {
    /// Fully synchronized and healthy.
    Ok = 1,
    /// Healthy but a few blocks behind the best head.
    Lagging = 2,
    /// Possibly valid, but too few peers to trust.
    Immature = 3,
    /// Performing initial sync, far behind the chain tip.
    Syncing = 4,
    /// Unreachable or otherwise broken.
    Unavailable = 5,
}

impl UpstreamAvailability {
    /// Every status, for reports that must cover the full range (e.g. metrics
    /// that publish zero counts for unused statuses).
    pub const ALL: [UpstreamAvailability; 5] = [
        UpstreamAvailability::Ok,
        UpstreamAvailability::Lagging,
        UpstreamAvailability::Immature,
        UpstreamAvailability::Syncing,
        UpstreamAvailability::Unavailable,
    ];

    /// Reconstruct from the `u8` discriminant stored in atomics.
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Ok,
            2 => Self::Lagging,
            3 => Self::Immature,
            4 => Self::Syncing,
            _ => Self::Unavailable,
        }
    }
}

impl fmt::Display for UpstreamAvailability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpstreamAvailability::Ok => write!(f, "OK"),
            UpstreamAvailability::Lagging => write!(f, "LAGGING"),
            UpstreamAvailability::Immature => write!(f, "IMMATURE"),
            UpstreamAvailability::Syncing => write!(f, "SYNCING"),
            UpstreamAvailability::Unavailable => write!(f, "UNAVAILABLE"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_matches_severity() {
        assert!(UpstreamAvailability::Ok < UpstreamAvailability::Lagging);
        assert!(UpstreamAvailability::Lagging < UpstreamAvailability::Unavailable);
    }

    #[test]
    fn display_matches_legacy_format() {
        assert_eq!(UpstreamAvailability::Ok.to_string(), "OK");
        assert_eq!(UpstreamAvailability::Lagging.to_string(), "LAGGING");
        assert_eq!(UpstreamAvailability::Unavailable.to_string(), "UNAVAILABLE");
    }
}
