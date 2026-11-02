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

//! The downstream metrics of a gRPC request.

use crate::blockchain::TargetBlockchain;
use crate::metrics::{self, ApiMethod, DownstreamProtocol, RequestStatus};
use std::time::{Duration, Instant};
use tonic::Code;

/// Times a gRPC request and records how it ended in the downstream metrics.
#[derive(Clone, Copy)]
pub struct DownstreamRequest {
    method: ApiMethod,
    started: Instant,
}

impl DownstreamRequest {
    /// Starts timing a request for `method`.
    pub fn start(method: ApiMethod) -> Self {
        Self {
            method,
            started: Instant::now(),
        }
    }

    /// Records the request as ended with `result` and passes it on. For a
    /// subscription, that's when it starts, or is refused.
    pub fn finish<T>(
        &self,
        chain: Option<&TargetBlockchain>,
        result: Result<T, tonic::Status>,
    ) -> Result<T, tonic::Status> {
        let status = match &result {
            Ok(_) => RequestStatus::Success,
            Err(status) => RequestStatus::from(status),
        };
        self.record(chain, status, Some(self.elapsed()));
        result
    }

    /// For a caller that takes the response time at a point of its own, like
    /// the first item of a stream.
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// Records the request as refused with `status` and passes it on.
    pub fn refused(
        &self,
        chain: Option<&TargetBlockchain>,
        status: tonic::Status,
    ) -> tonic::Status {
        self.record(chain, RequestStatus::from(&status), Some(self.elapsed()));
        status
    }

    /// Records the request as ended with `status`, answered after `elapsed`,
    /// or `None` if the client left before any answer.
    pub fn record(
        &self,
        chain: Option<&TargetBlockchain>,
        status: RequestStatus,
        elapsed: Option<Duration>,
    ) {
        metrics::downstream_request(
            DownstreamProtocol::Dshackle,
            self.method,
            chain,
            status,
            elapsed,
        );
    }
}

/// The client's own mistakes are an `error`; everything else means Dshackle
/// couldn't serve the request.
impl From<&tonic::Status> for RequestStatus {
    fn from(status: &tonic::Status) -> Self {
        match status.code() {
            Code::InvalidArgument
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::Unauthenticated
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented => RequestStatus::Error,
            _ => RequestStatus::Failed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_mistakes_are_errors() {
        let status = tonic::Status::invalid_argument("unknown chain id 7");
        assert_eq!(RequestStatus::from(&status), RequestStatus::Error);
        let status = tonic::Status::unimplemented("Method foo is not supported");
        assert_eq!(RequestStatus::from(&status), RequestStatus::Error);
    }

    #[test]
    fn unserved_requests_are_failures() {
        let status = tonic::Status::unavailable("no upstream available");
        assert_eq!(RequestStatus::from(&status), RequestStatus::Failed);
        let status = tonic::Status::internal("native_call task failed");
        assert_eq!(RequestStatus::from(&status), RequestStatus::Failed);
    }
}
