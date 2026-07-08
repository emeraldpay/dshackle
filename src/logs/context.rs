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

//! The client-request context propagated from the server entry points down to
//! the upstream calls, so the request log can attribute an upstream request to
//! the client request it serves. The legacy version carried the same values in
//! the Reactor context (`AccessContext` / `RequestContext`).

use crate::logs::record::{LogTimestamp, Remote, RequestDetails};
use std::future::Future;
use uuid::Uuid;

tokio::task_local! {
    static CURRENT: IngressContext;
}

/// Identity of one client request: shared by all access log records it
/// produces and referenced by the request log records of the upstream calls
/// made on its behalf.
#[derive(Clone, Debug)]
pub struct IngressContext {
    pub id: Uuid,
    pub start: LogTimestamp,
    pub remote: Option<Remote>,
}

impl IngressContext {
    pub fn new(remote: Option<Remote>) -> Self {
        Self {
            id: Uuid::new_v4(),
            start: LogTimestamp::now(),
            remote,
        }
    }

    /// The `request` object of an access log record.
    pub fn request_details(&self) -> RequestDetails {
        RequestDetails {
            id: self.id,
            start: self.start,
            remote: self.remote.clone(),
        }
    }
}

/// Run `f` with `context` as the current client request. Spawned tasks don't
/// inherit it — re-wrap the spawned future when the work still serves the
/// same request.
pub async fn with_context<F: Future>(context: IngressContext, f: F) -> F::Output {
    CURRENT.scope(context, f).await
}

/// The client request the current task is serving, if any. `None` means the
/// work is Dshackle-internal (head polls, validation probes).
pub fn current() -> Option<IngressContext> {
    CURRENT.try_with(|ctx| ctx.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn context_visible_inside_scope_only() {
        assert!(current().is_none());
        let ctx = IngressContext::new(None);
        let id = ctx.id;
        with_context(ctx, async move {
            let seen = current().expect("context is set");
            assert_eq!(seen.id, id);
        })
        .await;
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn spawned_tasks_do_not_inherit() {
        let ctx = IngressContext::new(None);
        with_context(ctx, async {
            let outside = tokio::spawn(async { current().is_none() }).await.unwrap();
            assert!(outside);
        })
        .await;
    }
}
