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

//! Metrics of the requests Dshackle makes to its upstreams.
//!
//! One family per measure, with the connection as the `protocol` label and
//! the outcome as the `status` label, instead of the legacy
//! `upstream.rpc.*` / `upstream.ws.*` / `upstream.grpc.*` families that
//! repeated each measure per connection type.

use super::status::RequestStatus;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, exponential_buckets,
};
use std::time::Duration;

/// The connection a request to an upstream went through, as the `protocol`
/// label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpstreamProtocol {
    /// JSON-RPC over HTTP (`http-json-rpc`).
    HttpJsonRpc,
    /// JSON-RPC over WebSocket (`ws-json-rpc`).
    WsJsonRpc,
    /// A remote Dshackle (`dshackle`).
    Dshackle,
    /// None: Dshackle settled the request itself before choosing a
    /// connection, e.g. refused it for the upstream's `limits` (`internal`).
    Internal,
}

impl UpstreamProtocol {
    fn label(self) -> &'static str {
        match self {
            UpstreamProtocol::HttpJsonRpc => "http-json-rpc",
            UpstreamProtocol::WsJsonRpc => "ws-json-rpc",
            UpstreamProtocol::Dshackle => "dshackle",
            UpstreamProtocol::Internal => "internal",
        }
    }
}

/// The outcomes of a request that went out over a connection. A remote
/// Dshackle may also answer `rate-limited`; a request held back by the local
/// limits never goes out, and has its own `internal` series.
const SENT: [RequestStatus; 3] = [
    RequestStatus::Success,
    RequestStatus::Error,
    RequestStatus::Failed,
];

/// Per-upstream request metrics, labeled by upstream id and chain.
pub struct UpstreamMetrics {
    requests: IntCounterVec,
    duration: HistogramVec,
    response_size: HistogramVec,
    in_flight: IntGaugeVec,
}

impl UpstreamMetrics {
    /// The metrics, not yet registered.
    pub fn new(prefix: &str) -> Self {
        // Response sizes from 128B to ~0.5GB; anything larger is effectively
        // "the last bucket" anyway.
        let size_buckets = exponential_buckets(128.0, 4.0, 12).expect("valid bucket definition");
        Self {
            requests: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_upstream_requests_total"),
                    "Number of requests to the upstream, by how they ended",
                ),
                &["upstream", "chain", "protocol", "status"],
            )
            .expect("valid metric definition"),
            duration: HistogramVec::new(
                HistogramOpts::new(
                    format!("{prefix}_upstream_request_duration_seconds"),
                    "Time the upstream took to answer a request",
                ),
                &["upstream", "chain", "protocol"],
            )
            .expect("valid metric definition"),
            response_size: HistogramVec::new(
                HistogramOpts::new(
                    format!("{prefix}_upstream_response_size_bytes"),
                    "Size of the results the upstream answered with",
                )
                .buckets(size_buckets),
                &["upstream", "chain", "protocol"],
            )
            .expect("valid metric definition"),
            in_flight: IntGaugeVec::new(
                Opts::new(
                    format!("{prefix}_upstream_requests_in_flight"),
                    "Number of requests sent to the upstream and not yet answered",
                ),
                &["upstream", "chain", "protocol"],
            )
            .expect("valid metric definition"),
        }
    }

    /// Adds the metrics to `registry`.
    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.requests.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.duration.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.response_size.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.in_flight.clone()))
            .expect("register once");
    }

    /// Create the connection's zero-valued series upfront, so a configured
    /// upstream is visible in the scrape before it served its first request —
    /// like the legacy metrics, which were registered when the connection was
    /// built.
    pub fn on_created(&self, protocol: UpstreamProtocol, upstream: &str, chain: &str) {
        let labels = &[upstream, chain, protocol.label()];
        for status in SENT {
            self.requests
                .with_label_values(&[upstream, chain, protocol.label(), status.label()]);
        }
        if protocol == UpstreamProtocol::Dshackle {
            self.requests.with_label_values(&[
                upstream,
                chain,
                protocol.label(),
                RequestStatus::RateLimited.label(),
            ]);
        }
        self.duration.with_label_values(labels);
        self.response_size.with_label_values(labels);
        self.in_flight.with_label_values(labels);
    }

    /// Like [`on_created`](Self::on_created), for an upstream configured with
    /// `limits`.
    pub fn on_limits_created(&self, upstream: &str, chain: &str) {
        self.requests.with_label_values(&[
            upstream,
            chain,
            UpstreamProtocol::Internal.label(),
            RequestStatus::RateLimited.label(),
        ]);
    }

    /// A request sent to the upstream ended with `status`.
    pub fn on_request(
        &self,
        protocol: UpstreamProtocol,
        upstream: &str,
        chain: &str,
        status: RequestStatus,
    ) {
        self.requests
            .with_label_values(&[upstream, chain, protocol.label(), status.label()])
            .inc();
    }

    /// The upstream's own `limits` refused a request before it was sent.
    pub fn on_limited(&self, upstream: &str, chain: &str) {
        self.requests
            .with_label_values(&[
                upstream,
                chain,
                UpstreamProtocol::Internal.label(),
                RequestStatus::RateLimited.label(),
            ])
            .inc();
    }

    /// The upstream answered a request, with a result or an error, in `elapsed`.
    pub fn on_answered(
        &self,
        protocol: UpstreamProtocol,
        upstream: &str,
        chain: &str,
        elapsed: Duration,
    ) {
        self.duration
            .with_label_values(&[upstream, chain, protocol.label()])
            .observe(elapsed.as_secs_f64());
    }

    /// Size of a result the upstream answered with.
    pub fn on_response_size(
        &self,
        protocol: UpstreamProtocol,
        upstream: &str,
        chain: &str,
        size: usize,
    ) {
        self.response_size
            .with_label_values(&[upstream, chain, protocol.label()])
            .observe(size as f64);
    }

    /// A request is about to be sent to the upstream.
    pub fn on_sent(&self, protocol: UpstreamProtocol, upstream: &str, chain: &str) {
        self.in_flight
            .with_label_values(&[upstream, chain, protocol.label()])
            .inc();
    }

    /// A sent request ended, in any way.
    pub fn on_finished(&self, protocol: UpstreamProtocol, upstream: &str, chain: &str) {
        self.in_flight
            .with_label_values(&[upstream, chain, protocol.label()])
            .dec();
    }
}
