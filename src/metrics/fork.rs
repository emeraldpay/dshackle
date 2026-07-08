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

//! Metrics of the fork detection (legacy `forkwatch.*`).

use prometheus::{IntCounterVec, Opts, Registry};

/// Counts of fork-choice verdicts, labeled by strategy (`type`), verdict
/// (`status`) and chain.
pub struct ForkMetrics {
    status: IntCounterVec,
}

impl ForkMetrics {
    pub fn new(prefix: &str) -> Self {
        Self {
            status: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_forkwatch_status_bychain_total"),
                    "Number of fork-choice verdicts per status",
                ),
                &["type", "status", "chain"],
            )
            .expect("valid metric definition"),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.status.clone()))
            .expect("register once");
    }

    pub fn on_status(&self, choice: &str, status: &str, chain: &str) {
        self.status.with_label_values(&[choice, status, chain]).inc();
    }

    /// Create the zero-valued series for every status upfront, like the legacy
    /// `ForkWatch` that registered a counter per status at construction.
    pub fn on_created(&self, choice: &str, statuses: &[&str], chain: &str) {
        for status in statuses {
            self.status.with_label_values(&[choice, status, chain]);
        }
    }
}
