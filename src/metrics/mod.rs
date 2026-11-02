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

//! Prometheus metrics: the registry, a recording facade, and the scrape endpoint.
//!
//! Metric names, labels and semantics replicate what the legacy JVM version
//! exposed (a Micrometer registry where every application metric carried a
//! `dshackle.` prefix), so existing dashboards and alerts keep working after
//! the migration. The exception is the request metrics, downstream (clients
//! to Dshackle) and upstream (Dshackle to its upstreams), which have a new
//! consistent set of names (see the `downstream` and `upstream` modules).
//! JVM-runtime and Netty-internal metrics are intentionally not reproduced.
//!
//! Call sites record through the free functions of this module; they are no-ops
//! until [`init`] is called, so none of the instrumented code needs to know
//! whether monitoring is configured.

mod downstream;
mod fork;
mod logs;
mod select;
mod server;
mod state;
mod status;
mod upstream;

pub use downstream::{ApiMethod, CallMethod, DownstreamProtocol};
pub use state::{ChainStatus, UpstreamStatus, UpstreamsStatus};
pub use status::RequestStatus;
pub use upstream::UpstreamProtocol;

use crate::blockchain::TargetBlockchain;
use crate::config::monitoring::MonitoringConfig;
use crate::logs::LogKind;
use lazy_static::lazy_static;
use prometheus::Registry;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Replicates the `dshackle.` name prefix the legacy Micrometer registry added
/// to every application metric.
const PREFIX: &str = "dshackle";

/// The `chain` label value for requests not related to any particular chain
/// (e.g. gRPC `Describe`), as the legacy version reported them.
const CHAIN_NA: &str = "NA";

static ENABLED: AtomicBool = AtomicBool::new(false);
/// Whether per-method metrics are recorded (`monitoring.extended`). Read once
/// when the metrics are created, because a Prometheus label set is fixed at
/// metric creation — this mirrors the legacy Standard vs Extended metrics
/// split.
static EXTENDED: AtomicBool = AtomicBool::new(false);

lazy_static! {
    static ref REGISTRY: Registry = Registry::new();
    static ref METRICS: Metrics = {
        let m = Metrics::new();
        m.downstream.register(&REGISTRY);
        m.upstream.register(&REGISTRY);
        m.fork.register(&REGISTRY);
        // The log pipeline self-metrics are an extended-only subset, like the
        // legacy `LogMetrics.Enabled` gated on `Global.metricsExtended`.
        if EXTENDED.load(Ordering::Relaxed) {
            m.logs.register(&REGISTRY);
            m.select.register(&REGISTRY);
        }
        m
    };
}

struct Metrics {
    downstream: downstream::DownstreamMetrics,
    upstream: upstream::UpstreamMetrics,
    fork: fork::ForkMetrics,
    logs: logs::LogsMetrics,
    select: select::SelectMetrics,
}

impl Metrics {
    fn new() -> Self {
        Self {
            downstream: downstream::DownstreamMetrics::new(
                PREFIX,
                EXTENDED.load(Ordering::Relaxed),
            ),
            upstream: upstream::UpstreamMetrics::new(PREFIX),
            fork: fork::ForkMetrics::new(PREFIX),
            logs: logs::LogsMetrics::new(PREFIX),
            select: select::SelectMetrics::new(PREFIX),
        }
    }
}

/// Whether the extended-only metrics record anything.
fn extended_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed) && EXTENDED.load(Ordering::Relaxed)
}

/// Enable metrics recording and start the Prometheus scrape endpoint (unless
/// disabled by the config). Must be called before any request is served —
/// recording functions silently drop values until then.
pub fn init(config: &MonitoringConfig) {
    if !config.enabled {
        return;
    }
    // EXTENDED must be set before the lazy METRICS is touched: it decides the
    // label sets the metrics are created with.
    EXTENDED.store(config.extended, Ordering::SeqCst);
    ENABLED.store(true, Ordering::SeqCst);
    lazy_static::initialize(&METRICS);
    if config.prometheus.enabled {
        server::start(&config.prometheus, &REGISTRY);
    }
}

/// Expose the current state of the configured upstreams (availability,
/// connected count, lag, height) as gauges computed at scrape time.
pub fn register_upstreams(source: Arc<dyn UpstreamsStatus>) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    let collector = state::UpstreamsCollector::new(PREFIX, source);
    if let Err(e) = REGISTRY.register(Box::new(collector)) {
        tracing::warn!("Failed to register upstream state metrics: {}", e);
    }
}

fn chain_label(chain: Option<&TargetBlockchain>) -> String {
    chain
        .map(|c| c.code())
        .unwrap_or_else(|| CHAIN_NA.to_string())
}

// ── Requests from clients (`downstream_*`) ──────────────────────────────────

/// A JSON-RPC call was executed for a client: a JSON-RPC proxy call, or one
/// item of a gRPC `NativeCall`. `None` chain is reported as `NA` (an unknown
/// chain id).
pub fn downstream_call(
    protocol: DownstreamProtocol,
    chain: Option<&TargetBlockchain>,
    method: CallMethod<'_>,
    status: RequestStatus,
    elapsed: Duration,
) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS
        .downstream
        .on_call(protocol, &chain_label(chain), method, status, elapsed);
}

/// A request for an API method other than a JSON-RPC call ended with `status`:
/// answered, refused, or a subscription started, after `elapsed`; or `None`
/// for one never answered at all. `None` chain is reported as `NA` (e.g.
/// `Describe`, which isn't chain-specific, or an unknown chain id).
pub fn downstream_request(
    protocol: DownstreamProtocol,
    method: ApiMethod,
    chain: Option<&TargetBlockchain>,
    status: RequestStatus,
    elapsed: Option<Duration>,
) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS
        .downstream
        .on_request(protocol, method, &chain_label(chain), status, elapsed);
}

/// A subscription pushed one message to the client.
pub fn downstream_subscription_message(
    protocol: DownstreamProtocol,
    method: ApiMethod,
    chain: Option<&TargetBlockchain>,
) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS
        .downstream
        .on_subscription_message(protocol, method, &chain_label(chain));
}

// ── Upstream selection (legacy `select.*`, extended only) ───────────────────

/// A selection was made: how many upstreams of each role tier were up for
/// consideration (legacy `FilteredApis.Monitoring.count*`).
pub fn select_exist(chain: &TargetBlockchain, primary: usize, secondary: usize, fallback: usize) {
    if !extended_enabled() {
        return;
    }
    METRICS
        .select
        .on_selection(&chain.code(), primary, secondary, fallback);
}

/// A routed request finished after calling `count` upstreams (legacy
/// `FilteredApis.Monitoring.tried`).
pub fn select_tried(chain: &TargetBlockchain, count: usize) {
    if !extended_enabled() {
        return;
    }
    METRICS.select.on_tried(&chain.code(), count);
}

// ── Requests to upstreams (`upstream_*`) ────────────────────────────────────

/// An upstream connection was configured. Creates its zero-valued series so
/// the upstream is visible in the scrape before its first request.
pub fn upstream_created(protocol: UpstreamProtocol, upstream: &str, chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS
        .upstream
        .on_created(protocol, upstream, &chain.code());
}

/// An upstream was configured with `limits`. Creates its zero-valued
/// `rate-limited` series, so the refusals are visible before the first one.
pub fn upstream_limits_created(upstream: &str, chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.upstream.on_limits_created(upstream, &chain.code());
}

/// A request is about to be sent to the upstream. Pair with
/// [`upstream_finished`] once it completes in any way.
pub fn upstream_sent(protocol: UpstreamProtocol, upstream: &str, chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.upstream.on_sent(protocol, upstream, &chain.code());
}

/// A request sent to the upstream ended with `status`.
pub fn upstream_finished(
    protocol: UpstreamProtocol,
    upstream: &str,
    chain: &TargetBlockchain,
    status: RequestStatus,
) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    let chain = chain.code();
    METRICS.upstream.on_finished(protocol, upstream, &chain);
    METRICS
        .upstream
        .on_request(protocol, upstream, &chain, status);
}

/// The upstream's own `limits` refused a request before it was sent.
pub fn upstream_limited(upstream: &str, chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.upstream.on_limited(upstream, &chain.code());
}

/// The upstream answered a request, with a result or an error, in `elapsed`.
pub fn upstream_answered(
    protocol: UpstreamProtocol,
    upstream: &str,
    chain: &TargetBlockchain,
    elapsed: Duration,
) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS
        .upstream
        .on_answered(protocol, upstream, &chain.code(), elapsed);
}

/// Size of a result the upstream answered with.
pub fn upstream_response_size(
    protocol: UpstreamProtocol,
    upstream: &str,
    chain: &TargetBlockchain,
    size: usize,
) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS
        .upstream
        .on_response_size(protocol, upstream, &chain.code(), size);
}

// ── Log pipeline self-metrics (`logs_*`, extended only) ─────────────────────

/// A log writer was started. Creates the log's zero-valued series so the
/// pipeline is visible in the scrape before its first event.
pub fn log_writer_created(log: LogKind) {
    if !extended_enabled() {
        return;
    }
    METRICS.logs.on_created(log);
}

/// A log event was submitted to a writer.
pub fn log_produced(log: LogKind) {
    if !extended_enabled() {
        return;
    }
    METRICS.logs.on_produced(log);
}

/// `count` log events were successfully written to the log target.
pub fn log_written(log: LogKind, count: u64) {
    if !extended_enabled() {
        return;
    }
    METRICS.logs.on_written(log, count);
}

/// A log event was dropped without writing (the queue was full).
pub fn log_dropped(log: LogKind) {
    if !extended_enabled() {
        return;
    }
    METRICS.logs.on_dropped(log);
}

/// The current number of log events waiting to be written.
pub fn log_queue_size(log: LogKind, size: u64) {
    if !extended_enabled() {
        return;
    }
    METRICS.logs.on_queue_size(log, size);
}

// ── Fork detection (`forkwatch_*`) ──────────────────────────────────────────

/// A fork watcher started for an upstream. Creates the zero-valued series for
/// every possible verdict, like the legacy per-status counters registered at
/// construction.
pub fn fork_watch_created(statuses: &[&str], chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.fork.on_created(statuses, &chain.code());
}

/// A fork choice classified an upstream's new block.
pub fn fork_status(status: &str, chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.fork.on_status(status, &chain.code());
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Gather the current scrape output. Test-only view into the otherwise
    /// write-only registry.
    pub(crate) fn scrape() -> String {
        let encoder = prometheus::TextEncoder::new();
        encoder
            .encode_to_string(&REGISTRY.gather())
            .expect("metrics encode to text")
    }

    /// Tests share one process-wide registry, so enable everything once.
    pub(crate) fn enable() {
        EXTENDED.store(false, Ordering::SeqCst);
        ENABLED.store(true, Ordering::SeqCst);
        lazy_static::initialize(&METRICS);
    }

    #[test]
    fn records_downstream_calls() {
        enable();
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        downstream_call(
            DownstreamProtocol::WsJsonRpc,
            Some(&chain),
            CallMethod::Supported("eth_blockNumber"),
            RequestStatus::Failed,
            Duration::from_millis(5),
        );

        // Without extended monitoring, every JSON-RPC call is a `nativeCall`.
        let out = scrape();
        assert!(out.contains(
            r#"dshackle_downstream_requests_total{chain="ETH",method="nativeCall",protocol="ws-json-rpc",status="failed"} 1"#
        ));
        assert!(out.contains(
            r#"dshackle_downstream_request_duration_seconds_count{chain="ETH",method="nativeCall",protocol="ws-json-rpc"} 1"#
        ));
    }

    #[test]
    fn records_grpc_requests() {
        enable();
        let chain: TargetBlockchain = "bitcoin".parse().unwrap();
        downstream_request(
            DownstreamProtocol::Dshackle,
            ApiMethod::EstimateFee,
            Some(&chain),
            RequestStatus::Success,
            Some(Duration::from_millis(3)),
        );
        downstream_request(
            DownstreamProtocol::Dshackle,
            ApiMethod::Describe,
            None,
            RequestStatus::Success,
            Some(Duration::from_millis(1)),
        );

        let out = scrape();
        assert!(out.contains(
            r#"dshackle_downstream_requests_total{chain="BTC",method="estimateFee",protocol="dshackle",status="success"} 1"#
        ));
        assert!(out.contains(
            r#"dshackle_downstream_request_duration_seconds_count{chain="BTC",method="estimateFee",protocol="dshackle"} 1"#
        ));
        assert!(out.contains(
            r#"dshackle_downstream_requests_total{chain="NA",method="describe",protocol="dshackle",status="success"} 1"#
        ));
    }

    #[test]
    fn records_grpc_subscriptions_without_duration() {
        enable();
        let chain: TargetBlockchain = "sepolia".parse().unwrap();
        downstream_request(
            DownstreamProtocol::Dshackle,
            ApiMethod::SubscribeHead,
            Some(&chain),
            RequestStatus::Success,
            Some(Duration::from_millis(1)),
        );
        downstream_subscription_message(
            DownstreamProtocol::Dshackle,
            ApiMethod::SubscribeHead,
            Some(&chain),
        );
        downstream_subscription_message(
            DownstreamProtocol::Dshackle,
            ApiMethod::SubscribeHead,
            Some(&chain),
        );

        let out = scrape();
        assert!(out.contains(
            r#"dshackle_downstream_requests_total{chain="SEPOLIA",method="subscribeHead",protocol="dshackle",status="success"} 1"#
        ));
        assert!(out.contains(
            r#"dshackle_downstream_subscription_messages_total{chain="SEPOLIA",method="subscribeHead",protocol="dshackle"} 2"#
        ));
        assert!(!out.contains(
            r#"dshackle_downstream_request_duration_seconds_count{chain="SEPOLIA",method="subscribeHead""#
        ));
    }

    #[test]
    fn records_upstream_metrics() {
        enable();
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        upstream_created(UpstreamProtocol::HttpJsonRpc, "metrics-up", &chain);
        upstream_sent(UpstreamProtocol::HttpJsonRpc, "metrics-up", &chain);
        let out = scrape();
        assert!(out.contains(
            r#"dshackle_upstream_requests_in_flight{chain="ETH",protocol="http-json-rpc",upstream="metrics-up"} 1"#
        ));
        assert!(out.contains(
            r#"dshackle_upstream_requests_total{chain="ETH",protocol="http-json-rpc",status="failed",upstream="metrics-up"} 0"#
        ));

        upstream_finished(
            UpstreamProtocol::HttpJsonRpc,
            "metrics-up",
            &chain,
            RequestStatus::Success,
        );
        upstream_answered(
            UpstreamProtocol::HttpJsonRpc,
            "metrics-up",
            &chain,
            Duration::from_millis(7),
        );
        upstream_response_size(UpstreamProtocol::HttpJsonRpc, "metrics-up", &chain, 1024);
        let out = scrape();
        assert!(out.contains(
            r#"dshackle_upstream_requests_in_flight{chain="ETH",protocol="http-json-rpc",upstream="metrics-up"} 0"#
        ));
        assert!(out.contains(
            r#"dshackle_upstream_requests_total{chain="ETH",protocol="http-json-rpc",status="success",upstream="metrics-up"} 1"#
        ));
        assert!(out.contains(
            r#"dshackle_upstream_request_duration_seconds_count{chain="ETH",protocol="http-json-rpc",upstream="metrics-up"} 1"#
        ));
        assert!(out.contains(
            r#"dshackle_upstream_response_size_bytes_count{chain="ETH",protocol="http-json-rpc",upstream="metrics-up"} 1"#
        ));
    }

    #[test]
    fn records_requests_refused_by_limits_as_internal() {
        enable();
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        upstream_limits_created("metrics-limited", &chain);
        assert!(scrape().contains(
            r#"dshackle_upstream_requests_total{chain="ETH",protocol="internal",status="rate-limited",upstream="metrics-limited"} 0"#
        ));

        upstream_limited("metrics-limited", &chain);
        assert!(scrape().contains(
            r#"dshackle_upstream_requests_total{chain="ETH",protocol="internal",status="rate-limited",upstream="metrics-limited"} 1"#
        ));
    }

    #[test]
    fn records_fork_verdicts_by_chain() {
        enable();
        let chain: TargetBlockchain = "ethereum-classic".parse().unwrap();
        fork_watch_created(&["new", "fallbehind"], &chain);
        fork_status("new", &chain);

        let out = scrape();
        assert!(out.contains(r#"dshackle_forkwatch_status_total{chain="ETC",status="new"} 1"#));
        assert!(
            out.contains(r#"dshackle_forkwatch_status_total{chain="ETC",status="fallbehind"} 0"#)
        );
    }

    #[test]
    fn remote_dshackle_has_limited_series_upfront() {
        enable();
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        upstream_created(UpstreamProtocol::Dshackle, "metrics-remote", &chain);

        assert!(scrape().contains(
            r#"dshackle_upstream_requests_total{chain="ETH",protocol="dshackle",status="rate-limited",upstream="metrics-remote"} 0"#
        ));
    }
}
