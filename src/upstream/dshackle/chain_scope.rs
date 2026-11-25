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

//! Which of the chains a remote Dshackle reports are routed to it.

use crate::blockchain::TargetBlockchain;
use crate::config::upstreams::Upstream;
use anyhow::anyhow;
use std::collections::HashSet;

/// The chains a remote Dshackle upstream is used for. A remote may serve more
/// chains than should be routed to it, e.g. a shared Dshackle of which only
/// some chains are meant for this server, so the config can limit them with
/// `blockchain` or `blockchains`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainScope {
    /// Every chain the remote reports, when the config names none.
    All,
    /// Only these chains; the others the remote reports are not joined.
    Only(HashSet<TargetBlockchain>),
}

impl ChainScope {
    pub fn includes(&self, chain: &TargetBlockchain) -> bool {
        match self {
            ChainScope::All => true,
            ChainScope::Only(chains) => chains.contains(chain),
        }
    }

    /// The configured chains that are not among the ones the remote
    /// `reported`, sorted by chain id for a stable log.
    pub fn unserved(&self, reported: &HashSet<TargetBlockchain>) -> Vec<TargetBlockchain> {
        let ChainScope::Only(chains) = self else {
            return Vec::new();
        };
        let mut unserved: Vec<_> = chains.difference(reported).copied().collect();
        unserved.sort_by_key(TargetBlockchain::id);
        unserved
    }
}

/// An unknown name is an error rather than skipped: dropping it would widen
/// a single-chain limit into no limit at all.
impl TryFrom<&Upstream> for ChainScope {
    type Error = anyhow::Error;

    fn try_from(upstream: &Upstream) -> Result<Self, Self::Error> {
        if upstream.blockchain.is_none() && upstream.blockchains.is_none() {
            return Ok(ChainScope::All);
        }
        upstream
            .chain_names()
            .map(|name| name.parse::<TargetBlockchain>().map_err(|e| anyhow!(e)))
            .collect::<Result<HashSet<_>, _>>()
            .map(ChainScope::Only)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::upstreams::{RawUpstream, process_raw_upstream};
    use emerald_api::proto::common::ChainRef;

    fn scope_of(chain_keys: &str) -> anyhow::Result<ChainScope> {
        let raw: RawUpstream = serde_yaml::from_str(&format!(
            "
id: remote
{chain_keys}
connection:
  dshackle:
    host: 10.2.0.15
    port: 2449
"
        ))
        .unwrap();
        ChainScope::try_from(&process_raw_upstream(raw).unwrap().unwrap())
    }

    fn only(chains: &[ChainRef]) -> ChainScope {
        ChainScope::Only(chains.iter().copied().map(TargetBlockchain::from).collect())
    }

    #[test]
    fn without_blockchains_takes_all() {
        assert_eq!(scope_of("").unwrap(), ChainScope::All);
    }

    #[test]
    fn single_blockchain_limits_to_it() {
        assert_eq!(
            scope_of("blockchain: ethereum").unwrap(),
            only(&[ChainRef::ChainEthereum])
        );
        assert_eq!(
            scope_of("chain: ethereum").unwrap(),
            only(&[ChainRef::ChainEthereum])
        );
    }

    #[test]
    fn blockchains_list_limits_to_them() {
        assert_eq!(
            scope_of("blockchains: [ethereum, bitcoin]").unwrap(),
            only(&[ChainRef::ChainEthereum, ChainRef::ChainBitcoin])
        );
        assert_eq!(
            scope_of("chains: [eth, sepolia]").unwrap(),
            only(&[ChainRef::ChainEthereum, ChainRef::ChainSepolia])
        );
    }

    #[test]
    fn unknown_blockchain_is_an_error() {
        let err = scope_of("blockchains: [ethereum, nochain]").unwrap_err();
        assert!(format!("{err:#}").contains("nochain"), "{err:#}");
    }

    #[test]
    fn unserved_lists_configured_chains_the_remote_lacks() {
        let scope = only(&[ChainRef::ChainEthereum, ChainRef::ChainBitcoin]);
        let reported = HashSet::from([TargetBlockchain::from(ChainRef::ChainEthereum)]);

        assert_eq!(
            scope.unserved(&reported),
            vec![TargetBlockchain::from(ChainRef::ChainBitcoin)]
        );
        assert!(ChainScope::All.unserved(&reported).is_empty());
    }
}
