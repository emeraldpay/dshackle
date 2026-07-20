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

//! Bitcoin-specific upstream implementations.

pub mod head;
pub mod http;
pub mod reader;
pub mod subscribe;
pub mod validator;
pub mod zmq;

/// Satoshis per Bitcoin (`COIN`).
const SATOSHIS_PER_BTC: f64 = 100_000_000.0;

/// Convert a decimal BTC JSON `value` to satoshis. Multiplying the `f64` by
/// `COIN` and rounding is exact for every valid amount (< 21M BTC ⇒ < 2^53
/// satoshis, well within `f64`'s integer range), matching the legacy
/// `BigDecimal * COIN` (`RpcUnspentDeserializer`, `BitcoinFees`) without a
/// decimal dependency. `None` for a missing or negative value.
pub fn btc_to_satoshis(value: &serde_json::Value) -> Option<u64> {
    let btc = value.as_f64()?;
    if btc < 0.0 {
        return None;
    }
    Some((btc * SATOSHIS_PER_BTC).round() as u64)
}
