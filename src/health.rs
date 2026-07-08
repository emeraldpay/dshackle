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

//! Health check HTTP endpoint. Ports the legacy `HealthCheckSetup`: a plain
//! text report answering 200 when every configured chain has the required
//! number of fully available upstreams, and 503 otherwise. A `?detailed` query
//! expands the report to every chain and upstream.

use crate::blockchain::TargetBlockchain;
use crate::config::health::HealthConfig;
use crate::upstream::UpstreamManager;
use crate::upstream::availability::UpstreamAvailability;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use warp::Filter;
use warp::http::Response;

/// One configured health requirement: a chain and how many of its upstreams
/// must be fully available.
struct ChainCheck {
    /// `None` when the config named an unknown blockchain — reported as
    /// `UNSPECIFIED` and always failing, as the legacy config reader did.
    chain: Option<TargetBlockchain>,
    min_available: u32,
}

impl ChainCheck {
    fn name(&self) -> &'static str {
        self.chain
            .as_ref()
            .map(|c| c.legacy_name())
            .unwrap_or("UNSPECIFIED")
    }
}

/// Verdict plus the report lines, matching the legacy `Detailed` pair.
struct Health {
    ok: bool,
    details: Vec<String>,
}

/// Start the health endpoint in a background task. Like the monitoring
/// endpoint, a failure to start is logged but doesn't stop the application.
pub fn start(config: &HealthConfig, upstreams: Arc<UpstreamManager>) {
    let ip: IpAddr = match config.host.parse() {
        Ok(ip) => ip,
        Err(_) => {
            tracing::error!(
                "Failed to start health server: invalid host {}",
                config.host
            );
            return;
        }
    };
    let addr = SocketAddr::new(ip, config.port);
    let path = config.path.clone();
    let checks = Arc::new(build_checks(config));
    tokio::spawn(async move {
        serve(addr, path, checks, upstreams).await;
    });
}

/// Resolve the configured chain names once at startup.
fn build_checks(config: &HealthConfig) -> Vec<ChainCheck> {
    config
        .blockchains
        .iter()
        .map(|c| {
            let chain = c.blockchain.parse::<TargetBlockchain>().ok();
            if chain.is_none() {
                // Same warning the legacy config reader logged.
                tracing::warn!("Using UNSPECIFIED blockchain for Health Check. Always fails");
            }
            ChainCheck {
                chain,
                min_available: c.min_available,
            }
        })
        .collect()
}

async fn serve(
    addr: SocketAddr,
    path: String,
    checks: Arc<Vec<ChainCheck>>,
    upstreams: Arc<UpstreamManager>,
) {
    let route = route(path.clone(), checks, upstreams);
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("Failed to start health server on {}: {}", addr, e);
            return;
        }
    };
    tracing::info!("Health check available at http://{}{}", addr, path);
    warp::serve(route).incoming(listener).run().await;
}

/// The request filter: the configured path (any method, like the legacy JDK
/// server), with the exact query string `detailed` switching to the expanded
/// report.
fn route(
    path: String,
    checks: Arc<Vec<ChainCheck>>,
    upstreams: Arc<UpstreamManager>,
) -> impl Filter<Extract = (Response<String>,), Error = warp::Rejection> + Clone {
    warp::path::full()
        .and(
            warp::query::raw()
                .or(warp::any().map(String::new))
                .unify(),
        )
        .and_then(move |full: warp::path::FullPath, query: String| {
            let checks = Arc::clone(&checks);
            let upstreams = Arc::clone(&upstreams);
            let path = path.clone();
            async move {
                if full.as_str() != path {
                    return Err(warp::reject::not_found());
                }
                let health = if query == "detailed" {
                    detailed_health(&checks, &upstreams)
                } else {
                    basic_health(&checks, &upstreams)
                };
                let code = if health.ok { 200 } else { 503 };
                Ok(Response::builder()
                    .status(code)
                    .body(health.details.join("\n"))
                    .expect("valid response"))
            }
        })
}

/// How many of the chain's upstreams are fully available (status `OK`).
fn count_available(upstreams: &UpstreamManager, chain: &TargetBlockchain) -> Option<u32> {
    let multistream = upstreams.get(chain)?;
    let count = multistream
        .upstreams()
        .iter()
        .filter(|u| u.availability() == UpstreamAvailability::Ok)
        .count();
    Some(count as u32)
}

/// The default report: one error line per failing requirement, or a single
/// `OK` (legacy `getHealth`).
fn basic_health(checks: &[ChainCheck], upstreams: &UpstreamManager) -> Health {
    let errors: Vec<String> = checks
        .iter()
        .filter_map(|check| {
            let available = check
                .chain
                .as_ref()
                .and_then(|chain| count_available(upstreams, chain));
            match available {
                None => Some(format!("{} UNAVAILABLE", check.name())),
                Some(count) if count < check.min_available => {
                    Some(format!("{} LACKS MIN AVAILABILITY", check.name()))
                }
                Some(_) => None,
            }
        })
        .collect();

    if errors.is_empty() {
        Health {
            ok: true,
            details: vec!["OK".to_string()],
        }
    } else {
        Health {
            ok: false,
            details: errors,
        }
    }
}

/// The expanded report: every configured chain with the state of each of its
/// upstreams, plus an error line for required-but-missing chains (legacy
/// `getDetailedHealth`).
fn detailed_health(checks: &[ChainCheck], upstreams: &UpstreamManager) -> Health {
    let chains = upstreams.chains();
    let is_configured =
        |check: &ChainCheck| check.chain.as_ref().is_some_and(|c| chains.contains(c));

    // Required chains with no upstreams at all are reported first.
    let mut details: Vec<String> = checks
        .iter()
        .filter(|check| !is_configured(check))
        .map(|check| format!("{} UNAVAILABLE", check.name()))
        .collect();
    let all_enabled = details.is_empty();

    let mut any_unavailable = false;
    for chain in &chains {
        let Some(multistream) = upstreams.get(chain) else {
            continue;
        };
        let required = checks.iter().find(|c| c.chain.as_ref() == Some(chain));
        let lacks_availability = required.is_some_and(|required| {
            count_available(upstreams, chain).unwrap_or(0) < required.min_available
        });
        any_unavailable |= lacks_availability;

        let status = if lacks_availability {
            "UNAVAILABLE"
        } else {
            "AVAILABLE"
        };
        details.push(format!("{} {}", chain.legacy_name(), status));
        for up in multistream.upstreams() {
            details.push(format!(
                "  {} {} with lag={}",
                up.id(),
                up.availability(),
                up.lag().unwrap_or(0)
            ));
        }
        if lacks_availability {
            details.push("  LACKS MIN AVAILABILITY".to_string());
        }
    }

    Health {
        ok: all_enabled && !any_unavailable,
        details,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::health::ChainHealthConfig;
    use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse, RpcMethod};
    use crate::upstream::Multistream;
    use crate::upstream::head::{Head, NoHead};
    use crate::upstream::quorum::{AlwaysQuorum, CallQuorum, QuorumFactory};
    use crate::upstream::state::UpstreamState;
    use crate::upstream::traits::{RpcUpstream, UpstreamError};
    use std::collections::HashMap;

    struct StubUpstream {
        id: &'static str,
        availability: UpstreamAvailability,
        lag: Option<u64>,
        state: Arc<UpstreamState>,
    }

    impl StubUpstream {
        fn new(
            id: &'static str,
            availability: UpstreamAvailability,
            lag: Option<u64>,
        ) -> Arc<dyn RpcUpstream> {
            Arc::new(Self {
                id,
                availability,
                lag,
                state: Arc::new(UpstreamState::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl RpcUpstream for StubUpstream {
        async fn call(&self, _: &JsonRpcRequest) -> Result<JsonRpcResponse, UpstreamError> {
            Err(UpstreamError::Transport("stub".into()))
        }
        fn id(&self) -> &str {
            self.id
        }
        fn availability(&self) -> UpstreamAvailability {
            self.availability
        }
        fn head(&self) -> &dyn Head {
            &NoHead
        }
        fn lag(&self) -> Option<u64> {
            self.lag
        }
        fn state(&self) -> &Arc<UpstreamState> {
            &self.state
        }
    }

    struct AlwaysFactory;
    impl QuorumFactory for AlwaysFactory {
        fn quorum_for(&self, _method: &RpcMethod) -> Box<dyn CallQuorum> {
            Box::new(AlwaysQuorum::new())
        }
    }

    fn manager(chains: Vec<(&str, Vec<Arc<dyn RpcUpstream>>)>) -> Arc<UpstreamManager> {
        let upstreams = chains
            .into_iter()
            .map(|(chain, ups)| {
                (
                    chain.parse::<TargetBlockchain>().unwrap(),
                    Arc::new(Multistream::new(ups, Arc::new(AlwaysFactory))),
                )
            })
            .collect();
        Arc::new(UpstreamManager::from_parts(upstreams, HashMap::new()))
    }

    fn checks(entries: Vec<(&str, u32)>) -> Vec<ChainCheck> {
        build_checks(&HealthConfig {
            blockchains: entries
                .into_iter()
                .map(|(blockchain, min_available)| ChainHealthConfig {
                    blockchain: blockchain.to_string(),
                    min_available,
                })
                .collect(),
            ..HealthConfig::default()
        })
    }

    #[test]
    fn healthy_when_enough_upstreams() {
        let manager = manager(vec![(
            "ethereum",
            vec![StubUpstream::new("local", UpstreamAvailability::Ok, Some(0))],
        )]);
        let health = basic_health(&checks(vec![("ethereum", 1)]), &manager);
        assert!(health.ok);
        assert_eq!(health.details, vec!["OK"]);
    }

    #[test]
    fn fails_below_min_available() {
        let manager = manager(vec![(
            "ethereum",
            vec![
                StubUpstream::new("a", UpstreamAvailability::Ok, Some(0)),
                StubUpstream::new("b", UpstreamAvailability::Syncing, Some(100)),
            ],
        )]);
        let health = basic_health(&checks(vec![("ethereum", 2)]), &manager);
        assert!(!health.ok);
        assert_eq!(health.details, vec!["ETHEREUM LACKS MIN AVAILABILITY"]);
    }

    #[test]
    fn fails_for_chain_without_upstreams() {
        let manager = manager(vec![(
            "ethereum",
            vec![StubUpstream::new("local", UpstreamAvailability::Ok, Some(0))],
        )]);
        let health = basic_health(&checks(vec![("ethereum", 1), ("bitcoin", 1)]), &manager);
        assert!(!health.ok);
        assert_eq!(health.details, vec!["BITCOIN UNAVAILABLE"]);
    }

    #[test]
    fn unknown_configured_chain_always_fails() {
        let manager = manager(vec![(
            "ethereum",
            vec![StubUpstream::new("local", UpstreamAvailability::Ok, Some(0))],
        )]);
        let health = basic_health(&checks(vec![("what-is-this", 1)]), &manager);
        assert!(!health.ok);
        assert_eq!(health.details, vec!["UNSPECIFIED UNAVAILABLE"]);
    }

    #[test]
    fn detailed_reports_every_chain_and_upstream() {
        let manager = manager(vec![
            (
                "ethereum",
                vec![
                    StubUpstream::new("local", UpstreamAvailability::Ok, Some(0)),
                    StubUpstream::new("remote", UpstreamAvailability::Syncing, Some(5)),
                ],
            ),
            (
                "bitcoin",
                vec![StubUpstream::new("btc1", UpstreamAvailability::Ok, None)],
            ),
        ]);
        let health = detailed_health(&checks(vec![("ethereum", 1)]), &manager);
        assert!(health.ok);
        // Chains are reported in chain-id order: Bitcoin (1) then Ethereum (100).
        assert_eq!(
            health.details,
            vec![
                "BITCOIN AVAILABLE",
                "  btc1 OK with lag=0",
                "ETHEREUM AVAILABLE",
                "  local OK with lag=0",
                "  remote SYNCING with lag=5",
            ]
        );
    }

    #[test]
    fn detailed_marks_chain_lacking_availability() {
        let manager = manager(vec![(
            "ethereum",
            vec![StubUpstream::new(
                "local",
                UpstreamAvailability::Unavailable,
                Some(0),
            )],
        )]);
        let health = detailed_health(&checks(vec![("ethereum", 1)]), &manager);
        assert!(!health.ok);
        assert_eq!(
            health.details,
            vec![
                "ETHEREUM UNAVAILABLE",
                "  local UNAVAILABLE with lag=0",
                "  LACKS MIN AVAILABILITY",
            ]
        );
    }

    #[test]
    fn detailed_reports_missing_required_chain_first() {
        let manager = manager(vec![(
            "ethereum",
            vec![StubUpstream::new("local", UpstreamAvailability::Ok, Some(0))],
        )]);
        let health = detailed_health(&checks(vec![("ethereum", 1), ("bitcoin", 1)]), &manager);
        assert!(!health.ok);
        assert_eq!(health.details[0], "BITCOIN UNAVAILABLE");
        assert_eq!(health.details[1], "ETHEREUM AVAILABLE");
    }

    #[tokio::test]
    async fn responds_200_when_healthy() {
        let manager = manager(vec![(
            "ethereum",
            vec![StubUpstream::new("local", UpstreamAvailability::Ok, Some(0))],
        )]);
        let route = route(
            "/health".to_string(),
            Arc::new(checks(vec![("ethereum", 1)])),
            manager,
        );

        let resp = warp::test::request().path("/health").reply(&route).await;
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.body(), "OK");
    }

    #[tokio::test]
    async fn responds_503_when_unhealthy() {
        let manager = manager(vec![(
            "ethereum",
            vec![StubUpstream::new(
                "local",
                UpstreamAvailability::Syncing,
                Some(0),
            )],
        )]);
        let route = route(
            "/health".to_string(),
            Arc::new(checks(vec![("ethereum", 1)])),
            manager,
        );

        let resp = warp::test::request().path("/health").reply(&route).await;
        assert_eq!(resp.status(), 503);
        assert_eq!(resp.body(), "ETHEREUM LACKS MIN AVAILABILITY");
    }

    #[tokio::test]
    async fn responds_detailed_on_query() {
        let manager = manager(vec![(
            "ethereum",
            vec![StubUpstream::new("local", UpstreamAvailability::Ok, Some(0))],
        )]);
        let route = route(
            "/health".to_string(),
            Arc::new(checks(vec![("ethereum", 1)])),
            manager,
        );

        let resp = warp::test::request()
            .path("/health?detailed")
            .reply(&route)
            .await;
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.body(), "ETHEREUM AVAILABLE\n  local OK with lag=0");
    }

    #[tokio::test]
    async fn responds_404_on_other_path() {
        let manager = manager(vec![]);
        let route = route("/health".to_string(), Arc::new(checks(vec![])), manager);

        let resp = warp::test::request().path("/other").reply(&route).await;
        assert_eq!(resp.status(), 404);
    }
}
