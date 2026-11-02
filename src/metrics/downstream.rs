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

//! Metrics of the downstream requests: those clients make to Dshackle, the
//! counterpart of the `upstream` ones Dshackle makes to its upstreams.
//!
//! The same families for the gRPC API and the JSON-RPC proxy, with the
//! protocol as a label, instead of the legacy `request.grpc.*` and
//! `request.jsonrpc.*` families.

use super::status::RequestStatus;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};
use std::time::Duration;

/// The protocol a client reached Dshackle with, as the `protocol` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DownstreamProtocol {
    /// Dshackle's own gRPC API (`dshackle`), the same protocol a Dshackle
    /// upstream is reached with.
    Dshackle,
    /// The JSON-RPC proxy over HTTP (`http-json-rpc`).
    HttpJsonRpc,
    /// The JSON-RPC proxy over WebSocket (`ws-json-rpc`).
    WsJsonRpc,
}

impl DownstreamProtocol {
    fn label(self) -> &'static str {
        match self {
            DownstreamProtocol::Dshackle => "dshackle",
            DownstreamProtocol::HttpJsonRpc => "http-json-rpc",
            DownstreamProtocol::WsJsonRpc => "ws-json-rpc",
        }
    }
}

/// A method of Dshackle's API, as the `method` label. Named after the gRPC
/// methods, in the camelCase the legacy `type` tag used. Whatever protocol
/// carried it, a JSON-RPC call is a [`NativeCall`](Self::NativeCall), and a
/// JSON-RPC subscription a [`NativeSubscribe`](Self::NativeSubscribe).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiMethod {
    NativeCall,
    NativeSubscribe,
    SubscribeHead,
    SubscribeTx,
    SubscribeBalance,
    SubscribeStatus,
    GetBalance,
    GetAllowance,
    SubscribeAllowance,
    EstimateFee,
    Describe,
}

impl ApiMethod {
    fn label(self) -> &'static str {
        match self {
            ApiMethod::NativeCall => "nativeCall",
            ApiMethod::NativeSubscribe => "nativeSubscribe",
            ApiMethod::SubscribeHead => "subscribeHead",
            ApiMethod::SubscribeTx => "subscribeTx",
            ApiMethod::SubscribeBalance => "subscribeBalance",
            ApiMethod::SubscribeStatus => "subscribeStatus",
            ApiMethod::GetBalance => "getBalance",
            ApiMethod::GetAllowance => "getAllowance",
            ApiMethod::SubscribeAllowance => "subscribeAllowance",
            ApiMethod::EstimateFee => "estimateFee",
            ApiMethod::Describe => "describe",
        }
    }

    /// A subscription has no response time to measure: it's open until the
    /// client leaves.
    fn is_subscription(self) -> bool {
        matches!(
            self,
            ApiMethod::NativeSubscribe
                | ApiMethod::SubscribeHead
                | ApiMethod::SubscribeTx
                | ApiMethod::SubscribeBalance
                | ApiMethod::SubscribeStatus
                | ApiMethod::SubscribeAllowance
        )
    }
}

/// The JSON-RPC method of a call, as far as the `method` label may name it.
///
/// The name comes from the client, so only methods the chain serves become
/// label values; anything else would let a client create new series at will
/// with made-up names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallMethod<'a> {
    /// A method the chain serves.
    Supported(&'a str),
    /// A method the chain doesn't serve, or not a method name at all
    /// (`unsupported`).
    Unsupported,
}

impl CallMethod<'_> {
    fn label(&self) -> &str {
        match self {
            CallMethod::Supported(method) => method,
            CallMethod::Unsupported => "unsupported",
        }
    }
}

/// Requests, their response time and subscription messages, labeled by
/// protocol, method and chain.
pub struct DownstreamMetrics {
    /// Whether a JSON-RPC call is labeled with its own method (`eth_call`)
    /// rather than `nativeCall`: the legacy Standard vs Extended metrics
    /// split, since a label per JSON-RPC method multiplies the series.
    extended: bool,
    requests: IntCounterVec,
    duration: HistogramVec,
    subscription_messages: IntCounterVec,
}

impl DownstreamMetrics {
    /// The metrics, labeling JSON-RPC calls by their own method when
    /// `extended`.
    pub fn new(prefix: &str, extended: bool) -> Self {
        Self {
            extended,
            requests: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_downstream_requests_total"),
                    "Number of requests from clients, by how they ended",
                ),
                &["protocol", "method", "chain", "status"],
            )
            .expect("valid metric definition"),
            duration: HistogramVec::new(
                HistogramOpts::new(
                    format!("{prefix}_downstream_request_duration_seconds"),
                    "Time to process a request and respond",
                ),
                &["protocol", "method", "chain"],
            )
            .expect("valid metric definition"),
            subscription_messages: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_downstream_subscription_messages_total"),
                    "Number of messages pushed to subscriptions",
                ),
                &["protocol", "method", "chain"],
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
            .register(Box::new(self.subscription_messages.clone()))
            .expect("register once");
    }

    /// A JSON-RPC call ended with `status` after `elapsed`.
    pub fn on_call(
        &self,
        protocol: DownstreamProtocol,
        chain: &str,
        method: CallMethod<'_>,
        status: RequestStatus,
        elapsed: Duration,
    ) {
        let method = if self.extended {
            method.label()
        } else {
            ApiMethod::NativeCall.label()
        };
        self.requests
            .with_label_values(&[protocol.label(), method, chain, status.label()])
            .inc();
        self.duration
            .with_label_values(&[protocol.label(), method, chain])
            .observe(elapsed.as_secs_f64());
    }

    /// A request for an API method other than a JSON-RPC call ended with
    /// `status` after `elapsed`.
    pub fn on_request(
        &self,
        protocol: DownstreamProtocol,
        method: ApiMethod,
        chain: &str,
        status: RequestStatus,
        elapsed: Duration,
    ) {
        self.requests
            .with_label_values(&[protocol.label(), method.label(), chain, status.label()])
            .inc();
        if !method.is_subscription() {
            self.duration
                .with_label_values(&[protocol.label(), method.label(), chain])
                .observe(elapsed.as_secs_f64());
        }
    }

    /// A subscription pushed one message to the client.
    pub fn on_subscription_message(
        &self,
        protocol: DownstreamProtocol,
        method: ApiMethod,
        chain: &str,
    ) {
        self.subscription_messages
            .with_label_values(&[protocol.label(), method.label(), chain])
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::TextEncoder;

    #[test]
    fn extended_labels_calls_by_supported_method_only() {
        let registry = Registry::new();
        let metrics = DownstreamMetrics::new("dshackle", true);
        metrics.register(&registry);

        for method in [CallMethod::Supported("eth_call"), CallMethod::Unsupported] {
            metrics.on_call(
                DownstreamProtocol::HttpJsonRpc,
                "ETH",
                method,
                RequestStatus::Success,
                Duration::from_millis(1),
            );
        }

        let out = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        assert!(out.contains(
            r#"dshackle_downstream_requests_total{chain="ETH",method="eth_call",protocol="http-json-rpc",status="success"} 1"#
        ));
        assert!(out.contains(
            r#"dshackle_downstream_requests_total{chain="ETH",method="unsupported",protocol="http-json-rpc",status="success"} 1"#
        ));
    }
}
