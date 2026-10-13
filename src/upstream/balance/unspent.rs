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

//! Where Bitcoin balances come from. A balance is the sum of an address's
//! unspent outputs, and there is more than one place to learn them: the node's
//! own wallet ([`super::rpc_unspent`]) or an Esplora address index
//! ([`super::esplora_unspent`]). Ports the legacy `UnspentReader`.

use std::collections::HashMap;

/// One unspent output backing part of an address's balance.
#[derive(Debug)]
pub struct Unspent {
    pub(super) address: String,
    pub(super) tx_id: String,
    pub(super) index: u64,
    /// Amount in satoshis.
    pub(super) value: u64,
}

/// A source of unspent outputs for Bitcoin addresses.
///
/// A failed read is an error rather than an empty answer: a balance built on
/// it would be a fabricated zero.
#[async_trait::async_trait]
pub trait UnspentSource: Send + Sync {
    /// The unspent outputs of `addresses`. An address with nothing unspent
    /// contributes no entries.
    async fn unspent(&self, addresses: &[String]) -> Result<Vec<Unspent>, tonic::Status>;

    /// The addresses among `addresses` that are in use, each with its unspent
    /// outputs. What counts as "in use" is the best signal the source has —
    /// an xpub scan relies on it to decide where a wallet ends.
    async fn used(
        &self,
        addresses: &[String],
    ) -> Result<HashMap<String, Vec<Unspent>>, tonic::Status>;

    /// How many addresses an xpub scan should hand to [`Self::used`] next.
    /// `until_gap` is how many more addresses the gap rule lets through if
    /// they all turn out unused, i.e. the smallest round that can end the
    /// scan; a source asks for more only when the extra addresses are free.
    fn scan_round(&self, until_gap: u32) -> u32;

    /// The most addresses one xpub scan may examine on this source, when it
    /// needs a tighter ceiling than the request validation applies. A scan
    /// asking for more is cut to it rather than rejected: the client continues
    /// from where it ended with the request's `start`.
    fn scan_limit(&self) -> Option<u32>;
}
