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

//! The downstream metrics of a gRPC request answered with a finite stream.

use crate::blockchain::TargetBlockchain;
use crate::metrics::RequestStatus;
use crate::rpc::downstream_request::DownstreamRequest;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// Passes a finite response stream through, and records the request once the
/// stream is over: a lazy stream does the work, and meets its errors, only as
/// it's read, so neither the handler's return nor the first item tells how
/// the request ended. Not for subscriptions, which never complete.
pub struct DownstreamStream<S> {
    inner: S,
    request: DownstreamRequest,
    chain: Option<TargetBlockchain>,
    /// The response time: until the first item, which is when the client
    /// starts getting an answer, however long the rest of the stream takes.
    first_item: Option<Duration>,
    settled: bool,
}

impl<S> DownstreamStream<S> {
    /// Records `request` for `chain` as `inner` ends.
    pub fn new(request: DownstreamRequest, chain: Option<TargetBlockchain>, inner: S) -> Self {
        Self {
            inner,
            request,
            chain,
            first_item: None,
            settled: false,
        }
    }

    /// Records how the stream ended, answered after `elapsed`, if at all.
    fn settle(&mut self, status: RequestStatus, elapsed: Option<Duration>) {
        if self.settled {
            return;
        }
        self.settled = true;
        self.request.record(self.chain.as_ref(), status, elapsed);
    }
}

impl<S, T> Stream for DownstreamStream<S>
where
    S: Stream<Item = Result<T, tonic::Status>> + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let polled = Pin::new(&mut this.inner).poll_next(cx);
        match &polled {
            Poll::Ready(Some(item)) => {
                if this.first_item.is_none() {
                    this.first_item = Some(this.request.elapsed());
                }
                // tonic ends the response at the first error, so it's the
                // outcome even if the stream had more to give.
                if let Err(status) = item {
                    this.settle(RequestStatus::from(status), this.first_item);
                }
            }
            // An empty stream is an answer too, given once it completes.
            Poll::Ready(None) => {
                let elapsed = this.first_item.unwrap_or_else(|| this.request.elapsed());
                this.settle(RequestStatus::Success, Some(elapsed));
            }
            Poll::Pending => {}
        }
        polled
    }
}

/// The client left, or its deadline passed, before the stream was over: the
/// request wasn't served, the same as an upstream request dropped midway. It's
/// timed only if it got as far as a first item; the wait of a client that got
/// nothing is no response time.
impl<S> Drop for DownstreamStream<S> {
    fn drop(&mut self) {
        self.settle(RequestStatus::Failed, self.first_item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{self, ApiMethod};
    use emerald_api::proto::common::ChainRef;
    use futures::StreamExt;

    // The registry is process-wide and tests run in parallel, so each test
    // uses a chain nothing else asks balances for.

    /// The count of `getBalance` requests on `chain` that ended with `status`.
    fn balance_requests(chain: ChainRef, status: &str) -> u64 {
        let chain = TargetBlockchain::from(chain).code();
        metrics::tests::scrape()
            .lines()
            .find(|line| {
                line.starts_with("dshackle_downstream_requests_total{")
                    && line.contains(&format!(r#"chain="{chain}""#))
                    && line.contains(r#"method="getBalance""#)
                    && line.contains(&format!(r#"status="{status}""#))
            })
            .and_then(|line| line.rsplit(' ').next()?.parse().ok())
            .unwrap_or(0)
    }

    fn balance_stream<S>(chain: ChainRef, inner: S) -> DownstreamStream<S> {
        let request = DownstreamRequest::start(ApiMethod::GetBalance);
        DownstreamStream::new(request, Some(chain.into()), inner)
    }

    #[tokio::test]
    async fn empty_stream_is_a_success() {
        metrics::tests::enable();
        let chain = ChainRef::ChainMorden;

        let items: Vec<Result<u32, tonic::Status>> =
            balance_stream(chain, futures::stream::empty())
                .collect()
                .await;

        assert!(items.is_empty());
        assert_eq!(balance_requests(chain, "success"), 1);
    }

    #[tokio::test]
    async fn error_after_results_is_the_outcome() {
        metrics::tests::enable();
        let chain = ChainRef::ChainKovan;
        let inner = futures::stream::iter(vec![
            Ok(1u32),
            Err(tonic::Status::unavailable(
                "no upstream for the second address",
            )),
        ]);

        let items: Vec<_> = balance_stream(chain, inner).collect().await;

        assert_eq!(items.len(), 2);
        assert_eq!(balance_requests(chain, "failed"), 1);
        assert_eq!(balance_requests(chain, "success"), 0);
    }

    #[tokio::test]
    async fn client_leaving_midway_is_a_failure() {
        metrics::tests::enable();
        let chain = ChainRef::ChainRopsten;
        let inner = futures::stream::iter(vec![Ok::<_, tonic::Status>(1u32), Ok(2)]);

        let mut stream = balance_stream(chain, inner);
        stream.next().await;
        drop(stream);

        assert_eq!(balance_requests(chain, "failed"), 1);
        assert_eq!(balance_requests(chain, "success"), 0);
    }

    #[tokio::test]
    async fn client_leaving_before_any_answer_is_not_timed() {
        metrics::tests::enable();
        let chain = ChainRef::ChainRinkeby;

        drop(balance_stream(
            chain,
            futures::stream::pending::<Result<u32, tonic::Status>>(),
        ));

        assert_eq!(balance_requests(chain, "failed"), 1);
        let code = TargetBlockchain::from(chain).code();
        assert!(!metrics::tests::scrape().contains(&format!(
            r#"dshackle_downstream_request_duration_seconds_count{{chain="{code}",method="getBalance""#
        )));
    }
}
