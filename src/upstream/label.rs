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

//! Validated labels of an upstream, used for label-based upstream selection.

use std::fmt;

/// One `name: value` label of an upstream.
///
/// Construction via [`new`](Self::new) is the only way to obtain one, so any
/// label held at runtime is proven valid: both sides trimmed, non-empty, and
/// limited to letters, digits, `-` and `_`. An empty label would be
/// unmatchable by a value selector yet still satisfy an existence selector —
/// a trap for the operator. Legacy only trims and silently drops empty
/// entries; the character set was always the intent there too, just never
/// enforced.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UpstreamLabel {
    name: String,
    value: String,
}

impl UpstreamLabel {
    /// Builds the label from its parts, trimming both. Either side being
    /// empty after the trim, or containing anything outside
    /// `[a-zA-Z0-9_-]`, is an error.
    pub fn new(name: &str, value: &str) -> Result<Self, InvalidUpstreamLabel> {
        let name = name.trim();
        let value = value.trim();
        if !is_valid_part(name) || !is_valid_part(value) {
            return Err(InvalidUpstreamLabel {
                name: name.to_string(),
                value: value.to_string(),
            });
        }
        Ok(Self {
            name: name.to_string(),
            value: value.to_string(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for UpstreamLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.value)
    }
}

fn is_valid_part(part: &str) -> bool {
    !part.is_empty()
        && part
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// A label pair that is empty on either side or steps outside the allowed
/// character set.
#[derive(Debug, Clone)]
pub struct InvalidUpstreamLabel {
    name: String,
    value: String,
}

impl fmt::Display for InvalidUpstreamLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid upstream label `{}: {}`: name and value must be non-empty and contain only letters, digits, `-` or `_`",
            self.name, self.value
        )
    }
}

impl std::error::Error for InvalidUpstreamLabel {}

/// One upstream's set of labels.
///
/// A local upstream has exactly one set — its configured labels — while a
/// Dshackle relay carries one set per node the remote advertises.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpstreamLabels(Vec<UpstreamLabel>);

impl UpstreamLabels {
    /// The value of the label with the given name, if present.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|l| l.name() == name)
            .map(UpstreamLabel::value)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, UpstreamLabel> {
        self.0.iter()
    }
}

impl FromIterator<UpstreamLabel> for UpstreamLabels {
    /// Collects labels keeping one entry per name, the last occurrence
    /// winning — how the legacy code collapsed duplicates by inserting them
    /// into a map in order.
    fn from_iter<T: IntoIterator<Item = UpstreamLabel>>(iter: T) -> Self {
        let mut labels: Vec<UpstreamLabel> = Vec::new();
        for label in iter {
            match labels.iter_mut().find(|l| l.name() == label.name()) {
                Some(existing) => *existing = label,
                None => labels.push(label),
            }
        }
        UpstreamLabels(labels)
    }
}

/// Builds a label set from `(name, value)` pairs, panicking on invalid ones.
#[cfg(test)]
pub(crate) fn test_labels(pairs: &[(&str, &str)]) -> UpstreamLabels {
    pairs
        .iter()
        .map(|(name, value)| UpstreamLabel::new(name, value).unwrap())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_and_trims() {
        let label = UpstreamLabel::new(" provider ", " infura ").unwrap();
        assert_eq!(label.name(), "provider");
        assert_eq!(label.value(), "infura");
    }

    #[test]
    fn rejects_empty_sides() {
        assert!(UpstreamLabel::new("", "value").is_err());
        assert!(UpstreamLabel::new("name", "").is_err());
        assert!(UpstreamLabel::new("  ", "value").is_err());
        assert!(UpstreamLabel::new("name", "  ").is_err());
    }

    #[test]
    fn rejects_characters_outside_the_set() {
        for (name, value) in [
            ("name with space", "value"),
            ("name", "value with space"),
            ("name.dot", "value"),
            ("name", "value/slash"),
            ("имя", "value"),
            ("name", "true!"),
        ] {
            assert!(
                UpstreamLabel::new(name, value).is_err(),
                "`{name}: {value}` must be invalid"
            );
        }
    }

    #[test]
    fn accepts_the_full_character_set() {
        assert!(UpstreamLabel::new("net_type-2", "us-east_1").is_ok());
    }

    #[test]
    fn duplicate_names_collapse_last_wins() {
        let labels: UpstreamLabels = [
            UpstreamLabel::new("archive", "false").unwrap(),
            UpstreamLabel::new("archive", "true").unwrap(),
        ]
        .into_iter()
        .collect();
        assert_eq!(labels.get("archive"), Some("true"));
        assert_eq!(labels.iter().count(), 1);
    }

    #[test]
    fn set_lookup_by_name() {
        let labels: UpstreamLabels = [
            UpstreamLabel::new("provider", "infura").unwrap(),
            UpstreamLabel::new("region", "us").unwrap(),
        ]
        .into_iter()
        .collect();
        assert_eq!(labels.get("provider"), Some("infura"));
        assert_eq!(labels.get("missing"), None);
        assert!(labels.contains("region"));
    }
}
