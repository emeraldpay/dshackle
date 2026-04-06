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

//! Configuration file discovery, parsing, and data types.

pub mod bytes;
pub mod cache;
pub mod env;
pub mod health;
pub mod log;
pub mod main_config;
pub mod monitoring;
pub mod proxy;
pub mod signature;
pub mod tls;
pub mod tokens;
pub mod upstreams;

pub use main_config::{read_config, MainConfig};

use std::path::{Path, PathBuf};

const DEFAULT_CONFIG: &str = "/etc/dshackle/dshackle.yaml";
const LOCAL_CONFIG: &str = "./dshackle.yaml";

/// Resolves the configuration file path.
///
/// Resolution order:
/// 1. Explicit path from CLI `--config-path` argument
/// 2. System default at `/etc/dshackle/dshackle.yaml`
/// 3. Local `./dshackle.yaml` in the working directory
///
/// Returns an error if no accessible config file is found.
pub fn resolve_config_path(explicit: Option<&Path>) -> Result<PathBuf, ConfigError> {
    if let Some(path) = explicit {
        let normalized = dunce::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        return if is_accessible(&normalized) {
            Ok(normalized)
        } else {
            Err(ConfigError::NotAccessible(normalized))
        };
    }

    let default_path = PathBuf::from(DEFAULT_CONFIG);
    if is_accessible(&default_path) {
        return Ok(dunce::canonicalize(&default_path).unwrap_or(default_path));
    }

    let local_path = PathBuf::from(LOCAL_CONFIG);
    if is_accessible(&local_path) {
        return Ok(dunce::canonicalize(&local_path).unwrap_or(local_path));
    }

    Err(ConfigError::NotFound)
}

fn is_accessible(path: &Path) -> bool {
    path.exists() && path.is_file()
}

/// Errors that can occur during config file resolution.
#[derive(Debug)]
pub enum ConfigError {
    /// No config file found at any of the default locations.
    NotFound,
    /// The explicitly provided path is not accessible.
    NotAccessible(PathBuf),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::NotFound => write!(
                f,
                "Configuration is not found at {DEFAULT_CONFIG} or {LOCAL_CONFIG}"
            ),
            ConfigError::NotAccessible(path) => {
                write!(
                    f,
                    "Configuration file does not exist or is not accessible: {}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn resolves_explicit_path() {
        let dir = tempfile::tempdir().unwrap();
        let config_file = dir.path().join("test.yaml");
        fs::write(&config_file, "version: v1").unwrap();

        let result = resolve_config_path(Some(&config_file));
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            dunce::canonicalize(&config_file).unwrap()
        );
    }

    #[test]
    fn fails_when_explicit_path_missing() {
        let result = resolve_config_path(Some(Path::new("/nonexistent/dshackle.yaml")));
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), ConfigError::NotAccessible(p) if p == Path::new("/nonexistent/dshackle.yaml"))
        );
    }

    #[test]
    fn fails_when_no_config_found() {
        if !is_accessible(Path::new(DEFAULT_CONFIG))
            && !is_accessible(Path::new(LOCAL_CONFIG))
        {
            let result = resolve_config_path(None);
            assert!(matches!(result.unwrap_err(), ConfigError::NotFound));
        }
    }
}
