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

//! Metrics of the gRPC API requests (legacy `request.grpc.*`).

use prometheus::{HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry};
use std::time::Duration;

/// The gRPC method a metric refers to, reported as the `type` label. The label
/// strings match the legacy tag values, which were camelCase method names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrpcRequestType {
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

impl GrpcRequestType {
    fn label(self) -> &'static str {
        match self {
            GrpcRequestType::NativeCall => "nativeCall",
            GrpcRequestType::NativeSubscribe => "nativeSubscribe",
            GrpcRequestType::SubscribeHead => "subscribeHead",
            GrpcRequestType::SubscribeTx => "subscribeTx",
            GrpcRequestType::SubscribeBalance => "subscribeBalance",
            GrpcRequestType::SubscribeStatus => "subscribeStatus",
            GrpcRequestType::GetBalance => "getBalance",
            GrpcRequestType::GetAllowance => "getAllowance",
            GrpcRequestType::SubscribeAllowance => "subscribeAllowance",
            GrpcRequestType::EstimateFee => "estimateFee",
            GrpcRequestType::Describe => "describe",
        }
    }
}

/// Request/response counters and response timing of the gRPC API, labeled by
/// method (`type`) and chain.
pub struct GrpcMetrics {
    request: IntCounterVec,
    response: IntCounterVec,
    response_err: IntCounterVec,
    response_time: HistogramVec,
    reply: IntCounterVec,
    fail: IntCounter,
}

impl GrpcMetrics {
    pub fn new(prefix: &str) -> Self {
        const LABELS: &[&str] = &["type", "chain"];
        Self {
            request: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_request_grpc_request_total"),
                    "Number of received requests",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            response: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_request_grpc_response_total"),
                    "Number of responses",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            response_err: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_request_grpc_response_err_total"),
                    "Number of error responses",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            response_time: HistogramVec::new(
                HistogramOpts::new(
                    format!("{prefix}_request_grpc_response_time_seconds"),
                    "Time to process a request and respond",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            reply: IntCounterVec::new(
                Opts::new(
                    format!("{prefix}_request_grpc_reply_total"),
                    "Number of messages pushed to subscriptions",
                ),
                LABELS,
            )
            .expect("valid metric definition"),
            fail: IntCounter::new(
                format!("{prefix}_request_grpc_fail_total"),
                "Number of requests failed with an unhandled error",
            )
            .expect("valid metric definition"),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.request.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.response.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.response_err.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.response_time.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.reply.clone()))
            .expect("register once");
        registry
            .register(Box::new(self.fail.clone()))
            .expect("register once");
    }

    pub fn on_request(&self, request: GrpcRequestType, chain: &str) {
        self.request.with_label_values(&[request.label(), chain]).inc();
    }

    pub fn on_response(&self, request: GrpcRequestType, chain: &str) {
        self.response
            .with_label_values(&[request.label(), chain])
            .inc();
    }

    pub fn on_response_err(&self, request: GrpcRequestType, chain: &str) {
        self.response_err
            .with_label_values(&[request.label(), chain])
            .inc();
    }

    pub fn on_response_time(&self, request: GrpcRequestType, chain: &str, elapsed: Duration) {
        self.response_time
            .with_label_values(&[request.label(), chain])
            .observe(elapsed.as_secs_f64());
    }

    pub fn on_reply(&self, request: GrpcRequestType, chain: &str) {
        self.reply.with_label_values(&[request.label(), chain]).inc();
    }

    pub fn on_fail(&self) {
        self.fail.inc();
    }
}
