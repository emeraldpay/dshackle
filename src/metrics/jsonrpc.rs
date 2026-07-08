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

//! Metrics of the JSON-RPC proxy requests (legacy `request.jsonrpc.*`).

use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};
use std::time::Duration;

/// Per-request metrics of the JSON-RPC proxy, labeled by chain — and by method
/// too when extended monitoring is on, which is the legacy
/// `StandardRequestMetrics` / `ExtendedRequestMetrics` distinction.
pub struct JsonRpcMetrics {
    extended: bool,
    call: HistogramVec,
    err: IntCounterVec,
    fail: IntCounterVec,
    request: IntCounterVec,
}

impl JsonRpcMetrics {
    pub fn new(prefix: &str, extended: bool) -> Self {
        let labels: &[&str] = if extended {
            &["chain", "method"]
        } else {
            &["chain"]
        };
        Self {
            extended,
            call: HistogramVec::new(
                HistogramOpts::new(
                    format!("{prefix}_request_jsonrpc_call_seconds"),
                    "Time to process a call",
                ),
                labels,
            )
            .expect("valid metric definition"),
            err: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_request_jsonrpc_err_total"),
                    "Number of requests ended with an error response",
                ),
                labels,
            )
            .expect("valid metric definition"),
            fail: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_request_jsonrpc_fail_total"),
                    "Number of requests failed to process",
                ),
                labels,
            )
            .expect("valid metric definition"),
            request: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_request_jsonrpc_request_total"),
                    "Number of requests",
                ),
                labels,
            )
            .expect("valid metric definition"),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.call.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.err.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.fail.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.request.clone()))
            .expect("register once");
    }

    /// The label values matching the label set chosen at creation.
    fn labels<'a>(&self, chain: &'a str, method: &'a str) -> Vec<&'a str> {
        if self.extended {
            vec![chain, method]
        } else {
            vec![chain]
        }
    }

    pub fn on_request(&self, chain: &str, method: &str) {
        self.request
            .with_label_values(&self.labels(chain, method))
            .inc();
    }

    pub fn on_call(&self, chain: &str, method: &str, elapsed: Duration) {
        self.call
            .with_label_values(&self.labels(chain, method))
            .observe(elapsed.as_secs_f64());
    }

    pub fn on_err(&self, chain: &str, method: &str) {
        self.err.with_label_values(&self.labels(chain, method)).inc();
    }

    pub fn on_fail(&self, chain: &str, method: &str) {
        self.fail
            .with_label_values(&self.labels(chain, method))
            .inc();
    }
}
