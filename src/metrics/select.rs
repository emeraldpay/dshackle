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

//! Metrics of the upstream selection (legacy `select.*` from `FilteredApis`).
//! Extended-only, like the legacy recording gated on `Global.metricsExtended`.

use prometheus::{HistogramOpts, HistogramVec, Registry, linear_buckets};

/// Per-selection distributions: how many upstreams of each role were up for
/// selection (`exist`), and how many were actually called before the request
/// resolved (`tried`).
pub struct SelectMetrics {
    exist: HistogramVec,
    tried: HistogramVec,
}

impl SelectMetrics {
    pub fn new(prefix: &str) -> Self {
        // Upstream counts are small integers; unit-wide buckets keep the
        // whole realistic range visible instead of a time-scaled default.
        let buckets = linear_buckets(1.0, 1.0, 10).expect("valid buckets");
        Self {
            exist: HistogramVec::new(
                HistogramOpts::new(
                    format!("{prefix}_select_exist"),
                    "Count of available upstreams to select",
                )
                .buckets(buckets.clone()),
                &["chain", "role"],
            )
            .expect("valid metric definition"),
            tried: HistogramVec::new(
                HistogramOpts::new(
                    format!("{prefix}_select_tried"),
                    "How many upstreams were checked",
                )
                .buckets(buckets),
                &["chain"],
            )
            .expect("valid metric definition"),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.exist.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.tried.clone()))
            .expect("register once");
    }

    pub fn on_selection(&self, chain: &str, primary: usize, secondary: usize, fallback: usize) {
        for (role, count) in [
            ("primary", primary),
            ("secondary", secondary),
            ("fallback", fallback),
        ] {
            self.exist
                .with_label_values(&[chain, role])
                .observe(count as f64);
        }
    }

    pub fn on_tried(&self, chain: &str, count: usize) {
        self.tried.with_label_values(&[chain]).observe(count as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::TextEncoder;

    #[test]
    fn records_with_legacy_names() {
        let registry = Registry::new();
        let metrics = SelectMetrics::new("dshackle");
        metrics.register(&registry);

        metrics.on_selection("ETH", 2, 1, 1);
        metrics.on_tried("ETH", 3);

        let out = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        assert!(out.contains(r#"dshackle_select_exist_count{chain="ETH",role="primary"} 1"#));
        assert!(out.contains(r#"dshackle_select_exist_sum{chain="ETH",role="primary"} 2"#));
        assert!(out.contains(r#"dshackle_select_exist_count{chain="ETH",role="secondary"} 1"#));
        assert!(out.contains(r#"dshackle_select_exist_count{chain="ETH",role="fallback"} 1"#));
        assert!(out.contains(r#"dshackle_select_tried_count{chain="ETH"} 1"#));
        assert!(out.contains(r#"dshackle_select_tried_sum{chain="ETH"} 3"#));
    }
}
