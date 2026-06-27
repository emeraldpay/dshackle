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

//! Environment variable substitution in config values.
//!
//! Replaces `${VAR_NAME}` patterns with the corresponding environment variable value.
//! If the variable is not set, it is replaced with an empty string.

use regex::Regex;
use std::sync::LazyLock;

static ENV_VAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{(\w+?)\}").expect("invalid env var regex"));

/// Substitutes `${VAR_NAME}` patterns in the given string with environment variable values.
pub fn substitute(value: &str) -> String {
    ENV_VAR_RE
        .replace_all(value, |caps: &regex::Captures| {
            let var_name = &caps[1];
            std::env::var(var_name).unwrap_or_default()
        })
        .into_owned()
}

/// Walks a YAML value tree and substitutes env variables in all string values.
pub fn substitute_in_yaml(value: &mut serde_yaml::Value) {
    match value {
        serde_yaml::Value::String(s) => {
            if s.contains("${") {
                *s = substitute(s);
            }
        }
        serde_yaml::Value::Mapping(m) => {
            for (_, v) in m.iter_mut() {
                substitute_in_yaml(v);
            }
        }
        serde_yaml::Value::Sequence(seq) => {
            for v in seq {
                substitute_in_yaml(v);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaves_plain_string_unchanged() {
        assert_eq!(substitute("hello world"), "hello world");
    }

    #[test]
    fn substitutes_known_env_var() {
        unsafe {
            std::env::set_var("DSHACKLE_TEST_VAR", "replaced");
        }
        assert_eq!(
            substitute("prefix_${DSHACKLE_TEST_VAR}_suffix"),
            "prefix_replaced_suffix"
        );
        unsafe {
            std::env::remove_var("DSHACKLE_TEST_VAR");
        }
    }

    #[test]
    fn replaces_unknown_var_with_empty() {
        assert_eq!(substitute("${DSHACKLE_NONEXISTENT_VAR_12345}"), "");
    }

    #[test]
    fn substitutes_multiple_vars() {
        unsafe {
            std::env::set_var("DSHACKLE_A", "one");
            std::env::set_var("DSHACKLE_B", "two");
        }
        assert_eq!(substitute("${DSHACKLE_A}-${DSHACKLE_B}"), "one-two");
        unsafe {
            std::env::remove_var("DSHACKLE_A");
            std::env::remove_var("DSHACKLE_B");
        }
    }
}
