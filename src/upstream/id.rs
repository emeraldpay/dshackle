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

//! Validated identifier of a configured upstream.

use regex::Regex;
use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

/// Same pattern as the legacy `UpstreamsConfigReader`: starts with a letter,
/// continues with letters/digits/hyphens/underscores, ends with a letter or a
/// digit. Minimum 3 characters.
static ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z][a-zA-Z0-9_-]+[a-zA-Z0-9]$").unwrap());

/// Identifier of an upstream, as set by the `id` field in the config.
///
/// Always satisfies the id pattern — construction via [`FromStr`] is the only
/// way to obtain one, so any `UpstreamId` held at runtime is proven valid. In
/// particular it can never contain `/`, which response signing relies on: the
/// signed message is `DSHACKLESIG/<nonce>/<upstream_id>/<hash>`, and an id
/// with `/` could forge the other fields (legacy rejects such ids in
/// `DefaultUpstream` for the same reason).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UpstreamId(String);

impl UpstreamId {
    /// The id exactly as written in the config, for boundaries that need a
    /// plain string (metric labels, TLS error contexts).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for UpstreamId {
    type Err = InvalidUpstreamId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if ID_RE.is_match(s) {
            Ok(UpstreamId(s.to_string()))
        } else {
            Err(InvalidUpstreamId(s.to_string()))
        }
    }
}

impl fmt::Display for UpstreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl PartialEq<str> for UpstreamId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for UpstreamId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

/// Shared id for the crate's test stubs that don't care about their identity,
/// so each stub doesn't have to keep its own static.
#[cfg(test)]
pub(crate) fn stub_id() -> &'static UpstreamId {
    static ID: LazyLock<UpstreamId> = LazyLock::new(|| "stub".parse().unwrap());
    &ID
}

/// Builds an id from a literal, panicking on an invalid one.
#[cfg(test)]
pub(crate) fn test_id(id: &str) -> UpstreamId {
    id.parse().unwrap()
}

/// A would-be upstream id that does not satisfy the id pattern.
#[derive(Debug, Clone)]
pub struct InvalidUpstreamId(String);

impl fmt::Display for InvalidUpstreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid upstream id `{}`: must be at least 3 characters, start with a letter, \
             contain only letters, digits, `-` or `_`, and end with a letter or a digit",
            self.0
        )
    }
}

impl std::error::Error for InvalidUpstreamId {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_typical_ids() {
        for id in [
            "infura",
            "local-node",
            "eth_archive-1",
            "up1",
            "my_upstream_1",
            "upstream-01",
            "a1b",
        ] {
            assert!(id.parse::<UpstreamId>().is_ok(), "{id} must be valid");
        }
    }

    #[test]
    fn rejects_short_ids() {
        for id in ["", "a", "ab"] {
            assert!(id.parse::<UpstreamId>().is_err(), "{id} must be invalid");
        }
    }

    #[test]
    fn rejects_bad_shapes() {
        for id in [
            "1abc",
            "-abc",
            "abc-",
            "test_",
            "ab c",
            "test.test",
            "test@test",
            "тест",
        ] {
            assert!(id.parse::<UpstreamId>().is_err(), "{id} must be invalid");
        }
    }

    #[test]
    fn rejects_slash() {
        // `/` is the separator of the signed-message format, so an id carrying
        // it could forge the other signed fields.
        assert!("bad/id".parse::<UpstreamId>().is_err());
    }

    #[test]
    fn displays_raw_id() {
        let id: UpstreamId = "infura".parse().unwrap();
        assert_eq!(id.to_string(), "infura");
        assert_eq!(id.as_str(), "infura");
    }
}
