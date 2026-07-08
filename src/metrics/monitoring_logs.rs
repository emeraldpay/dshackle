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

//! Self-metrics of the access/request log pipeline (legacy `monitoringLogs_*`,
//! registered only under extended monitoring). The `type` label is the log
//! category: `access` or `request`.

use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};

pub struct MonitoringLogsMetrics {
    produce: IntCounterVec,
    collect: IntCounterVec,
    drop: IntCounterVec,
    queue_size: IntGaugeVec,
}

impl MonitoringLogsMetrics {
    pub fn new(prefix: &str) -> Self {
        const LABELS: &[&str] = &["type"];
        Self {
            produce: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_monitoringLogs_produce_total"),
                    "Log events produced by Dshackle",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            collect: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_monitoringLogs_collect_total"),
                    "Log events successfully sent to a storage",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            drop: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_monitoringLogs_drop_total"),
                    "Log events dropped w/o sending",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            queue_size: IntGaugeVec::new(
                Opts::new(
                    format!("{prefix}_monitoringLogs_queueSize"),
                    "Log events queue size",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.produce.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.collect.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.drop.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.queue_size.clone()))
            .expect("register once");
    }

    /// Create the category's zero-valued series upfront, like the legacy
    /// counters registered at writer construction.
    pub fn on_created(&self, category: &str) {
        self.produce.with_label_values(&[category]);
        self.collect.with_label_values(&[category]);
        self.drop.with_label_values(&[category]);
        self.queue_size.with_label_values(&[category]);
    }

    pub fn on_produced(&self, category: &str) {
        self.produce.with_label_values(&[category]).inc();
    }

    pub fn on_collected(&self, category: &str, count: u64) {
        self.collect.with_label_values(&[category]).inc_by(count);
    }

    pub fn on_dropped(&self, category: &str) {
        self.drop.with_label_values(&[category]).inc();
    }

    pub fn on_queue_size(&self, category: &str, size: u64) {
        self.queue_size
            .with_label_values(&[category])
            .set(size as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::TextEncoder;

    #[test]
    fn records_with_legacy_names() {
        let registry = Registry::new();
        let metrics = MonitoringLogsMetrics::new("dshackle");
        metrics.register(&registry);

        metrics.on_produced("access");
        metrics.on_collected("access", 2);
        metrics.on_dropped("request");
        metrics.on_queue_size("access", 5);

        let out = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        assert!(out.contains(r#"dshackle_monitoringLogs_produce_total{type="access"} 1"#));
        assert!(out.contains(r#"dshackle_monitoringLogs_collect_total{type="access"} 2"#));
        assert!(out.contains(r#"dshackle_monitoringLogs_drop_total{type="request"} 1"#));
        assert!(out.contains(r#"dshackle_monitoringLogs_queueSize{type="access"} 5"#));
    }
}
