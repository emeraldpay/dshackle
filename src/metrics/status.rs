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

//! The `status` label shared by the request metrics of both sides: the
//! requests clients make to Dshackle, and those Dshackle makes to upstreams.

/// How a request ended, as the `status` label.
///
/// Kept to a few kinds, so a dashboard can tell answers from failures and
/// from the requests the `limits` held back, without the label growing with
/// every distinct error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestStatus {
    /// Answered with a result.
    Success,
    /// Answered with an error that belongs to the request, e.g. a reverted
    /// call, invalid params or an unsupported method.
    Error,
    /// No usable answer: a connection failed or timed out, an upstream
    /// replied with an HTTP error, an unparseable body, or said it's
    /// overloaded, or no upstream was available.
    Failed,
    /// Request limits refused it: an upstream's own `limits`, or those of
    /// every upstream that could serve it.
    RateLimited,
}

impl RequestStatus {
    pub(super) fn label(self) -> &'static str {
        match self {
            RequestStatus::Success => "success",
            RequestStatus::Error => "error",
            RequestStatus::Failed => "failed",
            RequestStatus::RateLimited => "rate-limited",
        }
    }
}
