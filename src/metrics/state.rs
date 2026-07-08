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

//! Gauges of the current upstream state (legacy `upstreams.*`).
//!
//! The legacy version registered Micrometer gauges that polled the live
//! upstream objects on every scrape. The same approach is used here: a custom
//! [`Collector`] takes a state snapshot at scrape time, so the gauges are
//! always current and never go stale between updates.

use crate::blockchain::TargetBlockchain;
use crate::upstream::availability::UpstreamAvailability;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntGaugeVec, Opts};
use std::sync::Arc;

/// The state snapshot the gauges are computed from. Implemented by the
/// upstream manager; defined here as a trait so the metrics don't depend on
/// the routing layer.
pub trait UpstreamsStatus: Send + Sync {
    fn chains_status(&self) -> Vec<ChainStatus>;
}

/// Current state of one chain's upstreams.
pub struct ChainStatus {
    pub chain: TargetBlockchain,
    pub upstreams: Vec<UpstreamStatus>,
}

/// Current state of one upstream.
pub struct UpstreamStatus {
    pub id: String,
    pub availability: UpstreamAvailability,
    pub lag: Option<u64>,
    pub height: Option<u64>,
}

/// Scrape-time collector of the `upstreams.*` gauges.
pub struct UpstreamsCollector {
    source: Arc<dyn UpstreamsStatus>,
    availability: IntGaugeVec,
    connected: IntGaugeVec,
    lag: IntGaugeVec,
    height: IntGaugeVec,
}

impl UpstreamsCollector {
    pub fn new(prefix: &str, source: Arc<dyn UpstreamsStatus>) -> Self {
        Self {
            source,
            availability: IntGaugeVec::new(
                Opts::new(
                    format!("{prefix}_upstreams_availability"),
                    "Number of upstreams in each availability status",
                ),
                &["chain", "status"],
            )
            .expect("valid metric definition"),
            connected: IntGaugeVec::new(
                Opts::new(
                    format!("{prefix}_upstreams_connected"),
                    "Number of connected upstreams",
                ),
                &["chain"],
            )
            .expect("valid metric definition"),
            lag: IntGaugeVec::new(
                Opts::new(
                    format!("{prefix}_upstreams_lag"),
                    "How many blocks the upstream is behind the chain head",
                ),
                &["chain", "upstream"],
            )
            .expect("valid metric definition"),
            height: IntGaugeVec::new(
                Opts::new(
                    format!("{prefix}_upstreams_height"),
                    "Current head height of the upstream",
                ),
                &["chain", "upstream"],
            )
            .expect("valid metric definition"),
        }
    }
}

impl Collector for UpstreamsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.availability
            .desc()
            .into_iter()
            .chain(self.connected.desc())
            .chain(self.lag.desc())
            .chain(self.height.desc())
            .collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // Rebuild all values from a fresh snapshot; reset first so upstreams
        // removed since the last scrape don't linger with stale values.
        self.availability.reset();
        self.connected.reset();
        self.lag.reset();
        self.height.reset();

        for chain in self.source.chains_status() {
            let chain_code = chain.chain.code();
            self.connected
                .with_label_values(&[&chain_code])
                .set(chain.upstreams.len() as i64);
            // Every status is reported, including zero counts, matching the
            // legacy gauges that were registered per status upfront.
            for status in UpstreamAvailability::ALL {
                let count = chain
                    .upstreams
                    .iter()
                    .filter(|up| up.availability == status)
                    .count();
                self.availability
                    .with_label_values(&[&chain_code, &status.to_string().to_lowercase()])
                    .set(count as i64);
            }
            for up in &chain.upstreams {
                self.lag
                    .with_label_values(&[&chain_code, &up.id])
                    .set(up.lag.unwrap_or(0) as i64);
                self.height
                    .with_label_values(&[&chain_code, &up.id])
                    .set(up.height.unwrap_or(0) as i64);
            }
        }

        let mut families = self.availability.collect();
        families.extend(self.connected.collect());
        families.extend(self.lag.collect());
        families.extend(self.height.collect());
        families
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Registry, TextEncoder};

    struct StubStatus;

    impl UpstreamsStatus for StubStatus {
        fn chains_status(&self) -> Vec<ChainStatus> {
            vec![ChainStatus {
                chain: "ethereum".parse().unwrap(),
                upstreams: vec![
                    UpstreamStatus {
                        id: "local".to_string(),
                        availability: UpstreamAvailability::Ok,
                        lag: Some(0),
                        height: Some(100),
                    },
                    UpstreamStatus {
                        id: "remote".to_string(),
                        availability: UpstreamAvailability::Syncing,
                        lag: Some(5),
                        height: Some(95),
                    },
                ],
            }]
        }
    }

    #[test]
    fn collects_current_state() {
        let registry = Registry::new();
        let collector = UpstreamsCollector::new("dshackle", Arc::new(StubStatus));
        registry.register(Box::new(collector)).unwrap();

        let out = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();

        assert!(out.contains(r#"dshackle_upstreams_connected{chain="ETH"} 2"#));
        assert!(out.contains(r#"dshackle_upstreams_availability{chain="ETH",status="ok"} 1"#));
        assert!(out.contains(r#"dshackle_upstreams_availability{chain="ETH",status="syncing"} 1"#));
        // Zero counts are reported too, like the legacy per-status gauges.
        assert!(out.contains(r#"dshackle_upstreams_availability{chain="ETH",status="lagging"} 0"#));
        assert!(out.contains(r#"dshackle_upstreams_lag{chain="ETH",upstream="remote"} 5"#));
        assert!(out.contains(r#"dshackle_upstreams_height{chain="ETH",upstream="local"} 100"#));
    }
}
