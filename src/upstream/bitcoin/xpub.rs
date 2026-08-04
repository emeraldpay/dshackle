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

//! BIP-32 extended-public-key ("xpub") address derivation, porting the legacy
//! `XpubAddresses`. The key's SLIP-132 prefix selects both the network and the
//! script type of the derived addresses. Children are the key's *direct*
//! non-hardened descendants (`key/i`), not a BIP-44 account structure — the
//! caller is expected to pass a chain-level key, same as legacy.
//!
//! Only derivation lives here; the balance scan built on top of it is
//! `upstream::balance::xpub_scan`. The legacy `activeAddresses` semantics
//! (activity from Esplora history, including used-but-empty addresses) are not
//! ported — that would need an address index this deployment doesn't run.

use bitcoin::address::Address;
use bitcoin::base58;
use bitcoin::bip32::{ChildNumber, Xpub};
use bitcoin::secp256k1::Secp256k1;
use bitcoin::{CompressedPublicKey, Network};
use std::fmt;
use std::str::FromStr;

/// BIP-32 serialization version bytes (SLIP-132): the four payload bytes that
/// produce the human-visible `xpub`/`tpub`/`zpub`/`vpub` Base58 prefixes.
const VERSION_XPUB: [u8; 4] = [0x04, 0x88, 0xB2, 0x1E];
const VERSION_TPUB: [u8; 4] = [0x04, 0x35, 0x87, 0xCF];
const VERSION_ZPUB: [u8; 4] = [0x04, 0xB2, 0x47, 0x46];
const VERSION_VPUB: [u8; 4] = [0x04, 0x5F, 0x1C, 0xF6];

/// Serialized BIP-32 key length after the Base58Check decode.
const KEY_LENGTH: usize = 78;

/// How derived public keys are rendered as addresses, as implied by the
/// SLIP-132 prefix of the extended key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ScriptKind {
    /// Pay-to-pubkey-hash (`xpub` / `tpub`).
    P2pkh,
    /// Native segwit pay-to-witness-pubkey-hash (`zpub` / `vpub`).
    P2wpkh,
}

/// Why an extended key cannot produce addresses.
#[derive(Debug, PartialEq, Eq)]
pub enum XpubError {
    /// The prefix is not one of the four supported kinds — e.g. the
    /// P2SH-wrapped `ypub`/`upub`, which legacy didn't support either.
    UnsupportedType(String),
    /// Not a Base58Check BIP-32 public key, or its version bytes don't match
    /// its own prefix.
    InvalidKey,
    /// A child index beyond the non-hardened range (>= 2^31), which a public
    /// key cannot derive.
    IndexOverflow(u32),
}

impl fmt::Display for XpubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // The legacy IllegalArgumentException wording.
            XpubError::UnsupportedType(prefix) => write!(f, "Unsupported type: {prefix}"),
            XpubError::InvalidKey => write!(f, "Not a valid extended public key"),
            XpubError::IndexOverflow(index) => {
                write!(
                    f,
                    "Derivation index {index} is out of the non-hardened range"
                )
            }
        }
    }
}

impl std::error::Error for XpubError {}

/// The address sequence defined by one extended public key (legacy
/// `XpubAddresses`): parse the key from its string form, then read addresses
/// by derivation index.
#[derive(Debug)]
pub struct XpubAddresses {
    key: Xpub,
    kind: ScriptKind,
    network: Network,
}

impl FromStr for XpubAddresses {
    type Err = XpubError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let prefix = s
            .get(..4)
            .ok_or_else(|| XpubError::UnsupportedType(s.to_string()))?;
        let (kind, network, version) = match prefix {
            "xpub" => (ScriptKind::P2pkh, Network::Bitcoin, VERSION_XPUB),
            "zpub" => (ScriptKind::P2wpkh, Network::Bitcoin, VERSION_ZPUB),
            "tpub" => (ScriptKind::P2pkh, Network::Testnet, VERSION_TPUB),
            "vpub" => (ScriptKind::P2wpkh, Network::Testnet, VERSION_VPUB),
            other => return Err(XpubError::UnsupportedType(other.to_string())),
        };
        let mut data = base58::decode_check(s).map_err(|_| XpubError::InvalidKey)?;
        if data.len() != KEY_LENGTH || data[..4] != version {
            return Err(XpubError::InvalidKey);
        }
        // The bitcoin crate only accepts the standard xpub/tpub versions, so a
        // SLIP-132 key is re-stamped with its network's standard bytes; the
        // script kind is already captured from the prefix.
        data[..4].copy_from_slice(&match network {
            Network::Bitcoin => VERSION_XPUB,
            _ => VERSION_TPUB,
        });
        let key = Xpub::decode(&data).map_err(|_| XpubError::InvalidKey)?;
        Ok(Self { key, kind, network })
    }
}

impl XpubAddresses {
    /// The network the key's prefix declares — lets callers reject a key that
    /// doesn't belong to the chain being queried before deriving anything.
    pub fn network(&self) -> Network {
        self.network
    }

    /// Addresses of the direct non-hardened children `key/start` ..
    /// `key/(start+limit-1)`, in derivation order (legacy `allAddresses`).
    pub fn addresses(&self, start: u32, limit: u32) -> Result<Vec<Address>, XpubError> {
        let secp = Secp256k1::verification_only();
        let mut out = Vec::with_capacity(limit as usize);
        for index in start..start.saturating_add(limit) {
            let child =
                ChildNumber::from_normal_idx(index).map_err(|_| XpubError::IndexOverflow(index))?;
            // With the hardened range already excluded, derivation fails only
            // for the ~2^-128 invalid-tweak child; treat it as the key having
            // no valid address there (legacy bitcoinj throws the same way).
            let key = self
                .key
                .ckd_pub(&secp, child)
                .map_err(|_| XpubError::InvalidKey)?;
            out.push(self.address_of(key.to_pub()));
        }
        Ok(out)
    }

    fn address_of(&self, pubkey: CompressedPublicKey) -> Address {
        match self.kind {
            ScriptKind::P2pkh => Address::p2pkh(pubkey.pubkey_hash(), self.network),
            ScriptKind::P2wpkh => Address::p2wpkh(&pubkey, self.network),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::PubkeyHash;
    use bitcoin::address::NetworkUnchecked;
    use bitcoin::hashes::Hash;

    // Keys and expected addresses are the legacy `XpubAddressesSpec` vectors
    // verbatim (accounts of the seed "seed sustain pyramid victory drum
    // primary silver safe wrestle section leg caught praise spring prepare
    // input"), so passing here means byte-compatible derivation.
    const MAINNET_ZPUB: &str = "zpub6tsHyQGj1UmDcw9UavB31HSChH9rqMi8Qje5KPbnczzhFSZykmm3afivpVytxWubaabDih6AbGGhioiTCg5PssYXuVGiZ1vTVaMYvvQvvmz";
    const MAINNET_ZPUB_2: &str = "zpub6tyKtxsQL16XpzxwFJSrVTxpKF4z8GL2WTqvdErC2iQBSEAzLqJNc8h6hg4EHMSPEavFDneR3JQjWjbZL7vtQrXMwN9bD4c9K9s1Z55aZfB";
    const TESTNET_VPUB: &str = "vpub5aEsY5bNGvjkHPjdMiz8FbdoiT81kmF3JwuJ1LRdoFb7DxNh1qAHQCmMeDnr6RG2EcCwzGohem3oESxZGa6YWLPW79ryCyMrdYj54uUzNNq";
    const TESTNET_VPUB_2: &str = "vpub5arxPHpfH2FKSNnBqyZJctzBtruGzM4sat7YKcQQNoNGgVZehD1tLiYGvhXBhPzKPcRDRjhGw94Dc9Wwob9BpbAMmkMX7Dzdfd5Ly9LHTGQ";

    fn derive(key: &str, start: u32, limit: u32) -> Vec<String> {
        key.parse::<XpubAddresses>()
            .unwrap()
            .addresses(start, limit)
            .unwrap()
            .iter()
            .map(|a| a.to_string())
            .collect()
    }

    #[test]
    fn mainnet_zpub_addresses() {
        assert_eq!(
            derive(MAINNET_ZPUB, 0, 4),
            [
                "bc1qd6p79j20w2zy5zagptf5ksjkdmhx4d5sykrz0e",
                "bc1q54um5vzdjuwt6qvzvk2y5em72jedpktrvr24a3",
                "bc1q3yl03ugy6l5k3te5x340m70hyeqjzmmvksnuc8",
                "bc1qp0cgpkxl52r82ev9k2wpdzzk4m4nflwurj5yr8",
            ]
        );
    }

    #[test]
    fn testnet_vpub_addresses() {
        assert_eq!(
            derive(TESTNET_VPUB, 0, 4),
            [
                "tb1q2phmcxl4tflgcrnm00jvrkh4a8n876vkjcv97m",
                "tb1qnhn0psv0uqtmg64tm258kqu0thurjvutcc80xs",
                "tb1qg62jwv77ul329pe4wcchr83zz02egwfh6syqsr",
                "tb1q8yg5qknrl5wmc98lw8dyfgrzamzaq8c9l0g44d",
            ]
        );
    }

    #[test]
    fn testnet_vpub_longer_run() {
        assert_eq!(
            derive(TESTNET_VPUB_2, 0, 12),
            [
                "tb1qepcagv9wkp04ygq3ud33qrkk6482ulhkegc333",
                "tb1qtt6qxjk8dfafpr24skplms0fs4kr5m08vvef3l",
                "tb1qmsu5p4jtzhpl097pwafz384meeep7gmv35udya",
                "tb1qc784y7urnu74x250vy70204gdqw32t5kd3z97c",
                "tb1q380kh7jhdgx5248uapp64pp8nltx654hhvs4hp",
                "tb1qert744aljx4t0w2y0crz0q9xhcn64u0t3vveel",
                "tb1q6d7v77wlceknrmc3u86c0j46ynltqs8n4pvvrd",
                "tb1q62y8sxclnvzlyk89nt7spfzwsx8v9qqfdsavwv",
                "tb1q3q0wlg5mjtjwc0rcedj5zhu2wc4rch23fpffg8",
                "tb1qhhl5fty3utag39gspac903sxmmdq2tsxs63vfy",
                "tb1qlse5sm59a8hckv2y7np60ka8egsw05wkj4nhal",
                "tb1ql7y7syltstfn3t5svd56ywff4j65cg0mjylcjp",
            ]
        );
    }

    #[test]
    fn starts_from_offset() {
        assert_eq!(
            derive(MAINNET_ZPUB_2, 10, 4),
            [
                "bc1q3dzm92mnqvkyadwl3x5cqdvxaqa5h9c37qdlgs",
                "bc1q3kqug4cx95a02yhwn6geelftmw3zklrgmhjll8",
                "bc1q6znmcw8zzjv3asdylgkmt0k0xvhzl9q5awh0sq",
                "bc1q3fzlxvw803r8j44culrt7jz7q5y7zakushujmq",
            ]
        );
    }

    #[test]
    fn zero_limit_is_empty() {
        assert_eq!(derive(MAINNET_ZPUB, 0, 0), Vec::<String>::new());
    }

    #[test]
    fn xpub_prefix_derives_p2pkh_of_the_same_keys() {
        // No legacy P2PKH vectors exist, so re-stamp the known zpub as an
        // xpub: the derived key hash must equal the witness program of the
        // known segwit address, rendered in base58 P2PKH form.
        let mut data = base58::decode_check(MAINNET_ZPUB).unwrap();
        data[..4].copy_from_slice(&VERSION_XPUB);
        let as_xpub = base58::encode_check(&data);
        assert!(as_xpub.starts_with("xpub"));

        let segwit = "bc1qd6p79j20w2zy5zagptf5ksjkdmhx4d5sykrz0e"
            .parse::<Address<NetworkUnchecked>>()
            .unwrap()
            .assume_checked();
        let hash =
            PubkeyHash::from_slice(segwit.witness_program().unwrap().program().as_bytes()).unwrap();
        assert_eq!(
            derive(&as_xpub, 0, 1),
            [Address::p2pkh(hash, Network::Bitcoin).to_string()]
        );
    }

    #[test]
    fn tpub_prefix_derives_testnet_p2pkh() {
        let mut data = base58::decode_check(TESTNET_VPUB).unwrap();
        data[..4].copy_from_slice(&VERSION_TPUB);
        let as_tpub = base58::encode_check(&data);
        assert!(as_tpub.starts_with("tpub"));

        let addresses = derive(&as_tpub, 0, 2);
        for address in addresses {
            // Testnet base58 P2PKH addresses start with 'm' or 'n'.
            assert!(
                address.starts_with('m') || address.starts_with('n'),
                "{address}"
            );
        }
    }

    #[test]
    fn rejects_unsupported_prefix() {
        // ypub is a real SLIP-132 kind (P2SH-wrapped segwit) that legacy
        // explicitly didn't support.
        assert_eq!(
            "ypub6Ww3ibxVfGzLrAH1PNcjyAWenMTbbAosGNB6VvmSEgytSER9azLDWCxoJwW7Ke7icmizBMXrzBx9979FfaHxHcrArf3zbeJJJUZPf663zsP"
                .parse::<XpubAddresses>()
                .unwrap_err(),
            XpubError::UnsupportedType("ypub".to_string())
        );
        assert_eq!(
            "xp".parse::<XpubAddresses>().unwrap_err(),
            XpubError::UnsupportedType("xp".to_string())
        );
    }

    #[test]
    fn rejects_corrupted_key() {
        // '0' is not a base58 character, so the checksum decode fails.
        assert_eq!(
            "xpub00invalid".parse::<XpubAddresses>().unwrap_err(),
            XpubError::InvalidKey
        );
    }

    #[test]
    fn rejects_hardened_range() {
        let key = MAINNET_ZPUB.parse::<XpubAddresses>().unwrap();
        assert_eq!(
            key.addresses(1 << 31, 1).unwrap_err(),
            XpubError::IndexOverflow(1 << 31)
        );
        // 2^31 - 1 is still a valid child; a range crossing the boundary
        // fails on the first hardened index.
        assert_eq!(
            key.addresses((1 << 31) - 2, 4).unwrap_err(),
            XpubError::IndexOverflow(1 << 31)
        );
    }
}
