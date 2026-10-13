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

//! Unspent outputs read from the node itself with `listunspent`, porting the
//! legacy `RpcUnspentReader`.
//!
//! The node answers only for addresses it tracks (wallet/watch-only, or an
//! address-indexed node) and returns a confident empty list for any other —
//! which is why this source is used only when the operator marked an upstream
//! `balance: true`. When several upstreams of a chain are marked so they must
//! be equivalent: the calls are routed like any others, and a node that
//! doesn't track the queried addresses would contribute empty answers.

use super::unspent::{Unspent, UnspentSource};
use crate::jsonrpc::JsonRpcRequest;
use crate::upstream::bitcoin::btc_to_satoshis;
use crate::upstream::egress::ChainAccess;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Addresses per `listunspent` call of an xpub scan. The node filters against
/// the whole set in a single wallet scan, so a call for 100 addresses costs
/// the same as a call for one.
const SCAN_BATCH: u32 = 100;

/// The chain's nodes as a source of unspent outputs.
pub struct RpcUnspentSource {
    access: Arc<dyn ChainAccess>,
}

impl RpcUnspentSource {
    /// A source calling `listunspent` through `access`, so each call is routed
    /// and retried like any other request to the chain.
    pub fn new(access: Arc<dyn ChainAccess>) -> Self {
        Self { access }
    }
}

#[async_trait::async_trait]
impl UnspentSource for RpcUnspentSource {
    /// One `listunspent(1, 9999999, [addresses])` call for the whole set. A
    /// failed call, an upstream error, or a missing result fails the read
    /// (legacy errors with `DataUnavailable`); an empty list is a valid zero
    /// balance.
    async fn unspent(&self, addresses: &[String]) -> Result<Vec<Unspent>, tonic::Status> {
        // An empty address array means "no filter" to Bitcoin Core, i.e. a dump of
        // the wallet's entire UTXO set — never what a caller with no addresses wants.
        if addresses.is_empty() {
            return Ok(Vec::new());
        }
        let request =
            JsonRpcRequest::new(0, "listunspent".into(), json!([1, 9_999_999, addresses]));
        let response = self
            .access
            .call(&request)
            .await
            .map_err(|e| tonic::Status::unavailable(format!("balance read failed: {e}")))?;
        if let Some(error) = &response.error {
            return Err(tonic::Status::unavailable(format!(
                "upstream error reading unspent outputs: {}",
                error.message
            )));
        }
        let result = response
            .result
            .ok_or_else(|| tonic::Status::unavailable("empty listunspent response"))?;
        let items = serde_json::from_str::<Vec<Value>>(result.get())
            .map_err(|e| tonic::Status::internal(format!("invalid listunspent result: {e}")))?;
        let requested: HashSet<&str> = addresses.iter().map(String::as_str).collect();
        Ok(items
            .iter()
            .filter_map(unspent)
            .filter(|u| requested.contains(u.address.as_str()))
            .collect())
    }

    /// `listunspent` cannot tell a used-then-emptied address from a never-used
    /// one, so the only activity signal is "currently has unspent outputs".
    async fn used(
        &self,
        addresses: &[String],
    ) -> Result<HashMap<String, Vec<Unspent>>, tonic::Status> {
        let mut by_address: HashMap<String, Vec<Unspent>> = HashMap::new();
        for output in self.unspent(addresses).await? {
            by_address
                .entry(output.address.clone())
                .or_default()
                .push(output);
        }
        Ok(by_address)
    }

    fn scan_round(&self, _until_gap: u32) -> u32 {
        SCAN_BATCH
    }

    fn scan_limit(&self) -> Option<u32> {
        None
    }
}

/// Parse one `listunspent` entry, dropping any with missing/invalid fields.
fn unspent(item: &Value) -> Option<Unspent> {
    Some(Unspent {
        address: item.get("address")?.as_str()?.to_string(),
        tx_id: item.get("txid")?.as_str()?.to_string(),
        index: item.get("vout")?.as_u64()?,
        value: btc_to_satoshis(item.get("amount")?)?,
    })
}
