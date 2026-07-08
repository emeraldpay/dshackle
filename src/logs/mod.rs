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

//! Access log (what clients requested) and request log (what Dshackle
//! requested from upstreams), written as JSONL to a file or a socket in the
//! format of the legacy implementation.
//!
//! Producers submit through the free functions of this module; they are cheap
//! no-ops until [`init`] enables a log, so the instrumented code doesn't need
//! to know the configuration.

pub mod access;
mod context;
mod encoding;
pub mod record;
pub mod request;
mod writer;

pub use context::{IngressContext, current, with_context};
pub use record::{Channel, Remote};

use crate::config::log::{AccessLogConfig, RequestLogConfig};
use serde::Serialize;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use writer::LogWriter;

static ACCESS: OnceLock<LogWriter> = OnceLock::new();
static REQUEST: OnceLock<LogWriter> = OnceLock::new();
static INCLUDE_MESSAGES: AtomicBool = AtomicBool::new(false);
static INCLUDE_PARAMS: AtomicBool = AtomicBool::new(false);

/// Start the configured log writers. Must be called before the servers start;
/// submissions before (or without) it are dropped silently.
pub fn init(access: &AccessLogConfig, request: &RequestLogConfig) {
    if access.enabled {
        INCLUDE_MESSAGES.store(access.include_messages, Ordering::SeqCst);
        if let Some(writer) = LogWriter::start(&access.target, "access") {
            tracing::info!(
                "Writing access log to {}",
                access.target.filename.as_deref().unwrap_or("socket")
            );
            let _ = ACCESS.set(writer);
        }
    } else {
        tracing::info!("Access Log is disabled");
    }
    if request.enabled {
        INCLUDE_PARAMS.store(request.include_params, Ordering::SeqCst);
        if let Some(writer) = LogWriter::start(&request.target, "request") {
            tracing::info!(
                "Writing request log to {}",
                request.target.filename.as_deref().unwrap_or("socket")
            );
            let _ = REQUEST.set(writer);
        }
    } else {
        tracing::info!("Request Log is disabled");
    }
}

/// Whether the access log records anything — lets producers skip building
/// records entirely.
pub fn access_enabled() -> bool {
    ACCESS.get().is_some()
}

/// Whether the request log records anything.
pub fn request_enabled() -> bool {
    REQUEST.get().is_some()
}

/// Whether access log records carry request/response payloads
/// (`include-messages`).
pub fn include_messages() -> bool {
    INCLUDE_MESSAGES.load(Ordering::Relaxed)
}

/// Whether request log records carry call params (`include-params`).
pub fn include_params() -> bool {
    INCLUDE_PARAMS.load(Ordering::Relaxed)
}

/// Submit one access log record.
pub fn access_log<T: Serialize>(record: &T) {
    submit(ACCESS.get(), record);
}

/// Submit one request log record.
pub fn request_log(record: &request::RequestRecord) {
    submit(REQUEST.get(), record);
}

fn submit<T: Serialize>(writer: Option<&LogWriter>, record: &T) {
    let Some(writer) = writer else {
        return;
    };
    match serde_json::to_vec(record) {
        Ok(event) => {
            writer.submit(event);
        }
        // A record that can't be serialized is skipped, never fails the request.
        Err(e) => tracing::warn!("Failed to serialize a log record: {}", e),
    }
}
