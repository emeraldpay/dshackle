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

//! Metrics of the connections to upstream nodes (legacy `upstream.*` and the
//! per-connection `RpcMetrics`).

use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, exponential_buckets,
};
use std::time::Duration;

/// The connection protocol used to reach an upstream, distinguishing the three
/// metric families the legacy version had (`upstream.rpc.*`, `upstream.ws.*`,
/// `upstream.grpc.*`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpstreamProtocol {
    /// JSON-RPC over HTTP.
    Rpc,
    /// JSON-RPC over WebSocket.
    Ws,
    /// A remote Dshackle over gRPC.
    Grpc,
}

/// The `conn` / `fail` / `response.size` triple of one connection protocol.
struct ProtocolMetrics {
    conn: HistogramVec,
    fail: IntCounterVec,
    response_size: HistogramVec,
}

impl ProtocolMetrics {
    /// `size_name` is passed in full because the legacy names are inconsistent:
    /// the WS and gRPC summaries carried a `Bytes` base unit (adding a `_bytes`
    /// suffix on export) while the HTTP one did not.
    fn new(conn_name: String, fail_name: String, size_name: String, what: &str) -> Self {
        // Response sizes from 128B to ~0.5GB; anything larger is effectively
        // "the last bucket" anyway.
        let size_buckets =
            exponential_buckets(128.0, 4.0, 12).expect("valid bucket definition");
        Self {
            conn: HistogramVec::new(
                HistogramOpts::new(conn_name, format!("Request time through {what}")),
                &["upstream", "chain"],
            )
            .expect("valid metric definition"),
            fail: IntCounterVec::new(
                Opts::new(fail_name, format!("Number of failures of {what}")),
                &["upstream", "chain"],
            )
            .expect("valid metric definition"),
            response_size: HistogramVec::new(
                HistogramOpts::new(size_name, format!("Size of responses through {what}"))
                    .buckets(size_buckets),
                &["upstream", "chain"],
            )
            .expect("valid metric definition"),
        }
    }

    fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.conn.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.fail.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.response_size.clone()))
            .expect("register once");
    }
}

/// Per-upstream connection metrics, labeled by upstream id and chain.
pub struct UpstreamMetrics {
    rpc: ProtocolMetrics,
    ws: ProtocolMetrics,
    grpc: ProtocolMetrics,
    queue_size: IntGaugeVec,
}

impl UpstreamMetrics {
    pub fn new(prefix: &str) -> Self {
        Self {
            rpc: ProtocolMetrics::new(
                format!("{prefix}_upstream_rpc_conn_seconds"),
                format!("{prefix}_upstream_rpc_fail_total"),
                // Unlike WS/gRPC, no `_bytes` suffix — the legacy HTTP summary
                // had no base unit.
                format!("{prefix}_upstream_rpc_response_size"),
                "a HTTP JSON RPC connection",
            ),
            ws: ProtocolMetrics::new(
                format!("{prefix}_upstream_ws_conn_seconds"),
                format!("{prefix}_upstream_ws_fail_total"),
                format!("{prefix}_upstream_ws_response_size_bytes"),
                "a WebSocket JSON RPC connection",
            ),
            grpc: ProtocolMetrics::new(
                format!("{prefix}_upstream_grpc_conn_seconds"),
                format!("{prefix}_upstream_grpc_fail_total"),
                format!("{prefix}_upstream_grpc_response_size_bytes"),
                "a Dshackle gRPC connection",
            ),
            queue_size: IntGaugeVec::new(
                Opts::new(
                    format!("{prefix}_upstream_rpc_queue_size"),
                    "Number of requests currently in flight to the upstream",
                ),
                &["upstream", "chain"],
            )
            .expect("valid metric definition"),
        }
    }

    pub fn register(&self, registry: &Registry) {
        self.rpc.register(registry);
        self.ws.register(registry);
        self.grpc.register(registry);
        registry
            .register(Box::new(self.queue_size.clone()))
            .expect("register once");
    }

    fn protocol(&self, protocol: UpstreamProtocol) -> &ProtocolMetrics {
        match protocol {
            UpstreamProtocol::Rpc => &self.rpc,
            UpstreamProtocol::Ws => &self.ws,
            UpstreamProtocol::Grpc => &self.grpc,
        }
    }

    /// Create the upstream's zero-valued series upfront, so a configured
    /// upstream is visible in the scrape before it served its first request —
    /// like the legacy metrics, which were registered when the connection was
    /// built.
    pub fn on_created(&self, protocol: UpstreamProtocol, upstream: &str, chain: &str) {
        let metrics = self.protocol(protocol);
        let labels = &[upstream, chain];
        metrics.conn.with_label_values(labels);
        metrics.fail.with_label_values(labels);
        metrics.response_size.with_label_values(labels);
        self.queue_size.with_label_values(labels);
    }

    pub fn on_call(
        &self,
        protocol: UpstreamProtocol,
        upstream: &str,
        chain: &str,
        elapsed: Duration,
    ) {
        self.protocol(protocol)
            .conn
            .with_label_values(&[upstream, chain])
            .observe(elapsed.as_secs_f64());
    }

    pub fn on_fail(&self, protocol: UpstreamProtocol, upstream: &str, chain: &str) {
        self.protocol(protocol)
            .fail
            .with_label_values(&[upstream, chain])
            .inc();
    }

    pub fn on_response_size(
        &self,
        protocol: UpstreamProtocol,
        upstream: &str,
        chain: &str,
        size: usize,
    ) {
        self.protocol(protocol)
            .response_size
            .with_label_values(&[upstream, chain])
            .observe(size as f64);
    }

    pub fn on_enqueued(&self, upstream: &str, chain: &str) {
        self.queue_size.with_label_values(&[upstream, chain]).inc();
    }

    pub fn on_finished(&self, upstream: &str, chain: &str) {
        self.queue_size.with_label_values(&[upstream, chain]).dec();
    }
}
