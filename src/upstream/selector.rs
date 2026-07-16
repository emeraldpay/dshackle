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

//! Label-based upstream selection, requested by gRPC clients through the
//! `Selector` message on `NativeCallRequest`.
//!
//! Port of the label half of the legacy `Selector` matchers. Method and
//! capability filtering stay in `Multistream`; this module only decides
//! whether an upstream's configured labels satisfy the client's selector
//! expression.

use crate::upstream::label::UpstreamLabels;
use emerald_api::proto::blockchain::{Selector, selector::SelectorType};

/// A predicate over an upstream's label map, built from the gRPC `Selector`
/// message. `Any` (the default) matches every upstream, so requests without a
/// selector are unaffected.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LabelSelector {
    /// No constraint — matches any upstream.
    #[default]
    Any,
    /// The label `name` is present and its value equals one of `values`.
    Label { name: String, values: Vec<String> },
    /// The label `name` is present, whatever its value.
    Exists(String),
    /// Every child selector matches.
    And(Vec<LabelSelector>),
    /// At least one child selector matches.
    Or(Vec<LabelSelector>),
    /// The child selector does not match.
    Not(Box<LabelSelector>),
}

impl LabelSelector {
    /// Whether any of the upstream's label sets satisfies this selector.
    ///
    /// The whole expression — negations included — is evaluated against one
    /// set at a time (legacy `LabelSelectorMatcher.matches(up)`): an upstream
    /// with no label sets at all matches nothing, not even a `Not`.
    pub fn matches_any_set(&self, sets: &[UpstreamLabels]) -> bool {
        sets.iter().any(|set| self.matches(set))
    }

    /// Whether the given label set satisfies this selector.
    pub fn matches(&self, labels: &UpstreamLabels) -> bool {
        match self {
            LabelSelector::Any => true,
            LabelSelector::Label { name, values } => labels
                .get(name)
                .is_some_and(|actual| values.iter().any(|v| v == actual)),
            LabelSelector::Exists(name) => labels.contains(name),
            LabelSelector::And(children) => children.iter().all(|c| c.matches(labels)),
            LabelSelector::Or(children) => children.iter().any(|c| c.matches(labels)),
            LabelSelector::Not(child) => !child.matches(labels),
        }
    }

    /// Convert the gRPC `Selector` message following the legacy
    /// `Selector.convertToMatcher` rules: a missing or unrecognized selector
    /// matches anything, and a label selector whose values are all blank
    /// degrades to an existence check.
    pub fn from_proto(selector: Option<&Selector>) -> LabelSelector {
        match selector {
            Some(s) => Self::convert(s),
            None => LabelSelector::Any,
        }
    }

    fn convert(selector: &Selector) -> LabelSelector {
        let Some(selector_type) = selector.selector_type.as_ref() else {
            return LabelSelector::Any;
        };
        match selector_type {
            SelectorType::LabelSelector(label) => {
                if label.name.is_empty() {
                    return LabelSelector::Any;
                }
                let has_values = label.value.iter().any(|v| !v.trim().is_empty());
                if has_values {
                    LabelSelector::Label {
                        name: label.name.clone(),
                        // The raw list is kept as-is (legacy passes the
                        // untrimmed values to `LabelMatcher`); trimming only
                        // decides between value match and existence check.
                        values: label.value.clone(),
                    }
                } else {
                    LabelSelector::Exists(label.name.clone())
                }
            }
            SelectorType::AndSelector(and) => {
                LabelSelector::And(and.selectors.iter().map(Self::convert).collect())
            }
            SelectorType::OrSelector(or) => {
                LabelSelector::Or(or.selectors.iter().map(Self::convert).collect())
            }
            SelectorType::NotSelector(not) => {
                LabelSelector::Not(Box::new(Self::from_proto(not.selector.as_deref())))
            }
            SelectorType::ExistsSelector(exists) => LabelSelector::Exists(exists.name.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use emerald_api::proto::blockchain::{
        AndSelector, ExistsSelector, LabelSelector as ProtoLabelSelector, NotSelector, OrSelector,
    };

    use crate::upstream::label::test_labels as labels;

    fn label_proto(name: &str, values: &[&str]) -> Selector {
        Selector {
            selector_type: Some(SelectorType::LabelSelector(ProtoLabelSelector {
                name: name.to_string(),
                value: values.iter().map(|v| v.to_string()).collect(),
            })),
        }
    }

    #[test]
    fn any_matches_everything() {
        assert!(LabelSelector::Any.matches(&labels(&[])));
        assert!(LabelSelector::Any.matches(&labels(&[("a", "b")])));
    }

    #[test]
    fn label_matches_one_of_values() {
        let sel = LabelSelector::Label {
            name: "provider".into(),
            values: vec!["infura".into(), "alchemy".into()],
        };
        assert!(sel.matches(&labels(&[("provider", "infura")])));
        assert!(sel.matches(&labels(&[("provider", "alchemy")])));
        assert!(!sel.matches(&labels(&[("provider", "local")])));
        assert!(!sel.matches(&labels(&[("region", "infura")])));
    }

    #[test]
    fn exists_checks_presence_only() {
        let sel = LabelSelector::Exists("archive".into());
        assert!(sel.matches(&labels(&[("archive", "false")])));
        assert!(!sel.matches(&labels(&[("full", "true")])));
    }

    #[test]
    fn and_requires_all() {
        let sel = LabelSelector::And(vec![
            LabelSelector::Exists("archive".into()),
            LabelSelector::Label {
                name: "region".into(),
                values: vec!["eu".into()],
            },
        ]);
        assert!(sel.matches(&labels(&[("archive", "true"), ("region", "eu")])));
        assert!(!sel.matches(&labels(&[("archive", "true"), ("region", "us")])));
        assert!(!sel.matches(&labels(&[("region", "eu")])));
    }

    #[test]
    fn or_requires_any() {
        let sel = LabelSelector::Or(vec![
            LabelSelector::Exists("archive".into()),
            LabelSelector::Exists("full".into()),
        ]);
        assert!(sel.matches(&labels(&[("archive", "true")])));
        assert!(sel.matches(&labels(&[("full", "true")])));
        assert!(!sel.matches(&labels(&[("light", "true")])));
    }

    #[test]
    fn not_negates() {
        let sel = LabelSelector::Not(Box::new(LabelSelector::Exists("archive".into())));
        assert!(!sel.matches(&labels(&[("archive", "true")])));
        assert!(sel.matches(&labels(&[])));
    }

    // ── label sets ───────────────────────────────────────────────────────

    #[test]
    fn any_set_matching_satisfies_the_selector() {
        // The shape of a Dshackle relay advertising several nodes: matching
        // one advertised node is enough (legacy `getLabels().any {...}`).
        let sel = LabelSelector::Label {
            name: "archive".into(),
            values: vec!["true".into()],
        };
        let sets = vec![
            labels(&[("archive", "false")]),
            labels(&[("archive", "true")]),
        ];
        assert!(sel.matches_any_set(&sets));
        assert!(!sel.matches_any_set(&sets[..1]));
    }

    #[test]
    fn no_label_sets_matches_nothing() {
        // Legacy evaluates the whole expression per set, so an upstream with
        // no sets at all fails even a negation.
        let sel = LabelSelector::Not(Box::new(LabelSelector::Exists("archive".into())));
        assert!(!sel.matches_any_set(&[]));
        // ...while a single empty set does satisfy the same negation.
        assert!(sel.matches_any_set(&[labels(&[])]));
    }

    // ── proto conversion ─────────────────────────────────────────────────

    #[test]
    fn from_proto_none_is_any() {
        assert_eq!(LabelSelector::from_proto(None), LabelSelector::Any);
        assert_eq!(
            LabelSelector::from_proto(Some(&Selector {
                selector_type: None
            })),
            LabelSelector::Any
        );
    }

    #[test]
    fn from_proto_label() {
        let sel = LabelSelector::from_proto(Some(&label_proto("provider", &["infura"])));
        assert_eq!(
            sel,
            LabelSelector::Label {
                name: "provider".into(),
                values: vec!["infura".into()],
            }
        );
    }

    #[test]
    fn from_proto_label_with_empty_name_is_any() {
        let sel = LabelSelector::from_proto(Some(&label_proto("", &["infura"])));
        assert_eq!(sel, LabelSelector::Any);
    }

    #[test]
    fn from_proto_label_with_blank_values_is_exists() {
        let sel = LabelSelector::from_proto(Some(&label_proto("archive", &["", "  "])));
        assert_eq!(sel, LabelSelector::Exists("archive".into()));
        let sel = LabelSelector::from_proto(Some(&label_proto("archive", &[])));
        assert_eq!(sel, LabelSelector::Exists("archive".into()));
    }

    #[test]
    fn from_proto_exists() {
        let sel = LabelSelector::from_proto(Some(&Selector {
            selector_type: Some(SelectorType::ExistsSelector(ExistsSelector {
                name: "archive".into(),
            })),
        }));
        assert_eq!(sel, LabelSelector::Exists("archive".into()));
    }

    #[test]
    fn from_proto_composite() {
        let proto = Selector {
            selector_type: Some(SelectorType::AndSelector(AndSelector {
                selectors: vec![
                    label_proto("provider", &["infura"]),
                    Selector {
                        selector_type: Some(SelectorType::NotSelector(Box::new(NotSelector {
                            selector: Some(Box::new(label_proto("region", &["us"]))),
                        }))),
                    },
                    Selector {
                        selector_type: Some(SelectorType::OrSelector(OrSelector {
                            selectors: vec![label_proto("archive", &["true"])],
                        })),
                    },
                ],
            })),
        };
        let sel = LabelSelector::from_proto(Some(&proto));
        assert!(sel.matches(&labels(&[
            ("provider", "infura"),
            ("region", "eu"),
            ("archive", "true"),
        ])));
        assert!(!sel.matches(&labels(&[
            ("provider", "infura"),
            ("region", "us"),
            ("archive", "true"),
        ])));
    }
}
