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
//! the migration. JVM-runtime and Netty-internal metrics are intentionally not
//! reproduced.
//!
//! Call sites record through the free functions of this module; they are no-ops
//! until [`init`] is called, so none of the instrumented code needs to know
//! whether monitoring is configured.

mod fork;
mod grpc;
mod jsonrpc;
mod monitoring_logs;
mod select;
mod server;
mod state;
mod upstream;

pub use grpc::GrpcRequestType;
pub use state::{ChainStatus, UpstreamStatus, UpstreamsStatus};
pub use upstream::UpstreamProtocol;

use crate::blockchain::TargetBlockchain;
use crate::config::monitoring::MonitoringConfig;
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
        m.jsonrpc.register(&REGISTRY);
        m.grpc.register(&REGISTRY);
        m.upstream.register(&REGISTRY);
        m.fork.register(&REGISTRY);
        // The log pipeline self-metrics are an extended-only subset, like the
        // legacy `LogMetrics.Enabled` gated on `Global.metricsExtended`.
        if EXTENDED.load(Ordering::Relaxed) {
            m.monitoring_logs.register(&REGISTRY);
            m.select.register(&REGISTRY);
        }
        m
    };
}

struct Metrics {
    jsonrpc: jsonrpc::JsonRpcMetrics,
    grpc: grpc::GrpcMetrics,
    upstream: upstream::UpstreamMetrics,
    fork: fork::ForkMetrics,
    monitoring_logs: monitoring_logs::MonitoringLogsMetrics,
    select: select::SelectMetrics,
}

impl Metrics {
    fn new() -> Self {
        Self {
            jsonrpc: jsonrpc::JsonRpcMetrics::new(PREFIX, EXTENDED.load(Ordering::Relaxed)),
            grpc: grpc::GrpcMetrics::new(PREFIX),
            upstream: upstream::UpstreamMetrics::new(PREFIX),
            fork: fork::ForkMetrics::new(PREFIX),
            monitoring_logs: monitoring_logs::MonitoringLogsMetrics::new(PREFIX),
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

// ── JSON-RPC proxy requests (legacy `request.jsonrpc.*`) ────────────────────

/// A JSON-RPC request was received by the proxy.
pub fn jsonrpc_request(chain: &TargetBlockchain, method: &str) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.jsonrpc.on_request(&chain.code(), method);
}

/// A JSON-RPC call finished processing (successfully or not) in `elapsed`.
pub fn jsonrpc_call(chain: &TargetBlockchain, method: &str, elapsed: Duration) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.jsonrpc.on_call(&chain.code(), method, elapsed);
}

/// A JSON-RPC call produced an error response.
pub fn jsonrpc_err(chain: &TargetBlockchain, method: &str) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.jsonrpc.on_err(&chain.code(), method);
}

/// A JSON-RPC call failed to process.
pub fn jsonrpc_fail(chain: &TargetBlockchain, method: &str) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.jsonrpc.on_fail(&chain.code(), method);
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

// ── gRPC API requests (legacy `request.grpc.*`) ─────────────────────────────

/// A gRPC request was received. `None` chain is reported as `NA` (e.g.
/// `Describe`, which isn't chain-specific).
pub fn grpc_request(request: GrpcRequestType, chain: Option<&TargetBlockchain>) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.grpc.on_request(request, &chain_label(chain));
}

/// A gRPC request produced its response.
pub fn grpc_response(request: GrpcRequestType, chain: Option<&TargetBlockchain>) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.grpc.on_response(request, &chain_label(chain));
}

/// A gRPC request produced an error response.
pub fn grpc_response_err(request: GrpcRequestType, chain: Option<&TargetBlockchain>) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.grpc.on_response_err(request, &chain_label(chain));
}

/// Time from receiving a gRPC request to its response.
pub fn grpc_response_time(
    request: GrpcRequestType,
    chain: Option<&TargetBlockchain>,
    elapsed: Duration,
) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS
        .grpc
        .on_response_time(request, &chain_label(chain), elapsed);
}

/// A gRPC subscription pushed one message to the client.
pub fn grpc_reply(request: GrpcRequestType, chain: Option<&TargetBlockchain>) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.grpc.on_reply(request, &chain_label(chain));
}

/// A gRPC request failed with an unhandled error.
pub fn grpc_fail() {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.grpc.on_fail();
}

// ── Upstream connections (legacy `upstream.*`) ──────────────────────────────

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

/// A request through an upstream connection completed in `elapsed`.
pub fn upstream_call(
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
        .on_call(protocol, upstream, &chain.code(), elapsed);
}

/// A request through an upstream connection failed.
pub fn upstream_fail(protocol: UpstreamProtocol, upstream: &str, chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.upstream.on_fail(protocol, upstream, &chain.code());
}

/// Size of a successful response from an upstream connection.
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

/// A request is about to be sent to the upstream. Pair with
/// [`upstream_finished`] once it completes in any way.
pub fn upstream_enqueued(upstream: &str, chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.upstream.on_enqueued(upstream, &chain.code());
}

/// A previously enqueued request completed (success, error or timeout).
pub fn upstream_finished(upstream: &str, chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.upstream.on_finished(upstream, &chain.code());
}

// ── Log pipeline self-metrics (legacy `monitoringLogs_*`, extended only) ────

/// A log writer was started. Creates the category's zero-valued series so the
/// pipeline is visible in the scrape before its first event.
pub fn log_writer_created(category: &'static str) {
    if !extended_enabled() {
        return;
    }
    METRICS.monitoring_logs.on_created(category);
}

/// A log event was submitted to a writer of the given category
/// (`access` / `request`).
pub fn log_produced(category: &'static str) {
    if !extended_enabled() {
        return;
    }
    METRICS.monitoring_logs.on_produced(category);
}

/// `count` log events were successfully written to the storage.
pub fn log_collected(category: &'static str, count: u64) {
    if !extended_enabled() {
        return;
    }
    METRICS.monitoring_logs.on_collected(category, count);
}

/// A log event was dropped without writing (the queue was full).
pub fn log_dropped(category: &'static str) {
    if !extended_enabled() {
        return;
    }
    METRICS.monitoring_logs.on_dropped(category);
}

/// The current number of log events waiting to be written.
pub fn log_queue_size(category: &'static str, size: u64) {
    if !extended_enabled() {
        return;
    }
    METRICS.monitoring_logs.on_queue_size(category, size);
}

// ── Fork detection (legacy `forkwatch.*`) ───────────────────────────────────

/// A fork watcher started for an upstream. Creates the zero-valued series for
/// every possible verdict, like the legacy per-status counters registered at
/// construction.
pub fn fork_watch_created(choice: &str, statuses: &[&str], chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.fork.on_created(choice, statuses, &chain.code());
}

/// A fork choice classified an upstream's new block.
pub fn fork_status(choice: &str, status: &str, chain: &TargetBlockchain) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.fork.on_status(choice, status, &chain.code());
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
    fn records_jsonrpc_metrics() {
        enable();
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        jsonrpc_request(&chain, "eth_blockNumber");
        jsonrpc_call(&chain, "eth_blockNumber", Duration::from_millis(5));
        jsonrpc_err(&chain, "eth_blockNumber");
        jsonrpc_fail(&chain, "eth_blockNumber");

        let out = scrape();
        assert!(out.contains(r#"dshackle_request_jsonrpc_request_total{chain="ETH"}"#));
        assert!(out.contains(r#"dshackle_request_jsonrpc_err_total{chain="ETH"}"#));
        assert!(out.contains(r#"dshackle_request_jsonrpc_fail_total{chain="ETH"}"#));
        assert!(out.contains(r#"dshackle_request_jsonrpc_call_seconds_count{chain="ETH"}"#));
    }

    #[test]
    fn records_grpc_metrics() {
        enable();
        let chain: TargetBlockchain = "bitcoin".parse().unwrap();
        grpc_request(GrpcRequestType::NativeCall, Some(&chain));
        grpc_response(GrpcRequestType::NativeCall, Some(&chain));
        grpc_response_err(GrpcRequestType::NativeCall, Some(&chain));
        grpc_response_time(
            GrpcRequestType::NativeCall,
            Some(&chain),
            Duration::from_millis(3),
        );
        grpc_request(GrpcRequestType::Describe, None);
        grpc_reply(GrpcRequestType::SubscribeHead, Some(&chain));
        grpc_fail();

        let out = scrape();
        assert!(
            out.contains(r#"dshackle_request_grpc_request_total{chain="BTC",type="nativeCall"}"#)
        );
        assert!(
            out.contains(r#"dshackle_request_grpc_response_total{chain="BTC",type="nativeCall"}"#)
        );
        assert!(out.contains(
            r#"dshackle_request_grpc_response_err_total{chain="BTC",type="nativeCall"}"#
        ));
        assert!(out.contains(
            r#"dshackle_request_grpc_response_time_seconds_count{chain="BTC",type="nativeCall"}"#
        ));
        assert!(out.contains(r#"dshackle_request_grpc_request_total{chain="NA",type="describe"}"#));
        assert!(
            out.contains(r#"dshackle_request_grpc_reply_total{chain="BTC",type="subscribeHead"}"#)
        );
        assert!(out.contains("dshackle_request_grpc_fail_total 1"));
    }

    #[test]
    fn records_upstream_metrics() {
        enable();
        let chain: TargetBlockchain = "ethereum".parse().unwrap();
        upstream_call(
            UpstreamProtocol::Rpc,
            "local",
            &chain,
            Duration::from_millis(7),
        );
        upstream_fail(UpstreamProtocol::Ws, "local", &chain);
        upstream_response_size(UpstreamProtocol::Grpc, "local", &chain, 1024);
        upstream_enqueued("local", &chain);

        let out = scrape();
        assert!(
            out.contains(
                r#"dshackle_upstream_rpc_conn_seconds_count{chain="ETH",upstream="local"}"#
            )
        );
        assert!(out.contains(r#"dshackle_upstream_ws_fail_total{chain="ETH",upstream="local"}"#));
        assert!(out.contains(
            r#"dshackle_upstream_grpc_response_size_bytes_count{chain="ETH",upstream="local"}"#
        ));
        assert!(
            out.contains(r#"dshackle_upstream_rpc_queue_size{chain="ETH",upstream="local"} 1"#)
        );
        upstream_finished("local", &chain);
        assert!(
            scrape()
                .contains(r#"dshackle_upstream_rpc_queue_size{chain="ETH",upstream="local"} 0"#)
        );
    }
}
