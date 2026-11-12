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

//! Token authentication for consuming the public Emerald API through a
//! dshackle connection. A connection to another Dshackle doesn't need it.
//!
//! ```yaml
//! connection:
//!   dshackle:
//!     url: https://api.emrld.io
//!     emerald-auth:
//!       token: ${EMERALD_TOKEN}
//! ```

use anyhow::{Result, bail};
use emerald_api::creds::Credentials;
use serde::Deserialize;
use std::fmt::{Debug, Formatter};

/// The `emerald-auth` section of a dshackle connection.
#[derive(Debug, Clone, Deserialize)]
pub struct EmeraldAuth {
    pub token: AuthToken,
}

impl EmeraldAuth {
    /// An unset `${VAR}` substitutes to an empty value, which would otherwise
    /// surface only as a rejected call to the remote much later.
    ///
    /// Not part of the parsing, so a disabled upstream doesn't stop the start.
    pub fn validate(&self) -> Result<()> {
        if self.token.0.trim().is_empty() {
            bail!("`emerald-auth.token` is empty (unset environment variable?)");
        }
        Ok(())
    }
}

/// A secret Auth Token, which the connection exchanges for a JWT on its first
/// call (and renews the JWT afterward on its own).
///
/// Never printed, so the config can be logged without leaking it.
#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct AuthToken(String);

impl Debug for AuthToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthToken(***)")
    }
}

impl From<&EmeraldAuth> for Credentials {
    fn from(auth: &EmeraldAuth) -> Self {
        Credentials::token(auth.token.0.trim())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_token() {
        let auth: EmeraldAuth = serde_yaml::from_str("token: abc123").unwrap();
        assert_eq!(auth.token, AuthToken("abc123".to_string()));
        auth.validate().unwrap();
    }

    #[test]
    fn empty_token_is_invalid() {
        let auth: EmeraldAuth = serde_yaml::from_str("token: ' '").unwrap();
        let err = auth.validate().unwrap_err();
        assert!(
            err.to_string().contains("`emerald-auth.token` is empty"),
            "{err}"
        );
    }

    #[test]
    fn debug_hides_token() {
        let auth: EmeraldAuth = serde_yaml::from_str("token: abc123").unwrap();
        assert!(!format!("{auth:?}").contains("abc123"));
    }
}
