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

//! Self-metrics of the access/request log pipeline (`logs_*`, legacy
//! `monitoringLogs_*`), registered only under extended monitoring.

use crate::logs::LogKind;
use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};

/// Self-metrics of the log writers, labeled by the `log` they write
/// (`access` or `request`).
pub struct LogsMetrics {
    produced: IntCounterVec,
    written: IntCounterVec,
    dropped: IntCounterVec,
    queue_size: IntGaugeVec,
}

impl LogsMetrics {
    /// The metrics, not yet registered.
    pub fn new(prefix: &str) -> Self {
        const LABELS: &[&str] = &["log"];
        Self {
            produced: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_logs_produced_total"),
                    "Log events produced by Dshackle",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            written: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_logs_written_total"),
                    "Log events written to the log target",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            dropped: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_logs_dropped_total"),
                    "Log events dropped without writing, because the queue was full",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            queue_size: IntGaugeVec::new(
                Opts::new(
                    format!("{prefix}_logs_queue_size"),
                    "Log events waiting to be written",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
        }
    }

    /// Adds the metrics to `registry`.
    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.produced.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.written.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.dropped.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.queue_size.clone()))
            .expect("register once");
    }

    /// Create the log's zero-valued series upfront, like the legacy counters
    /// registered at writer construction.
    pub fn on_created(&self, log: LogKind) {
        let labels = &[log.name()];
        self.produced.with_label_values(labels);
        self.written.with_label_values(labels);
        self.dropped.with_label_values(labels);
        self.queue_size.with_label_values(labels);
    }

    /// An event was submitted to the writer.
    pub fn on_produced(&self, log: LogKind) {
        self.produced.with_label_values(&[log.name()]).inc();
    }

    /// `count` events were written to the log target.
    pub fn on_written(&self, log: LogKind, count: u64) {
        self.written.with_label_values(&[log.name()]).inc_by(count);
    }

    /// An event was dropped because the queue was full.
    pub fn on_dropped(&self, log: LogKind) {
        self.dropped.with_label_values(&[log.name()]).inc();
    }

    /// The number of events waiting to be written.
    pub fn on_queue_size(&self, log: LogKind, size: u64) {
        self.queue_size
            .with_label_values(&[log.name()])
            .set(size as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::TextEncoder;

    #[test]
    fn records_by_log() {
        let registry = Registry::new();
        let metrics = LogsMetrics::new("dshackle");
        metrics.register(&registry);

        metrics.on_produced(LogKind::Access);
        metrics.on_written(LogKind::Access, 2);
        metrics.on_dropped(LogKind::Request);
        metrics.on_queue_size(LogKind::Access, 5);

        let out = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        assert!(out.contains(r#"dshackle_logs_produced_total{log="access"} 1"#));
        assert!(out.contains(r#"dshackle_logs_written_total{log="access"} 2"#));
        assert!(out.contains(r#"dshackle_logs_dropped_total{log="request"} 1"#));
        assert!(out.contains(r#"dshackle_logs_queue_size{log="access"} 5"#));
    }
}
