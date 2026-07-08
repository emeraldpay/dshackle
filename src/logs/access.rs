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

//! Access log (egress log) records: what clients requested from Dshackle and
//! what it replied. One record per reply message. The JSON schema replicates
//! the legacy `AccessRecord` classes field by field, including field order.

use crate::blockchain::TargetBlockchain;
use crate::logs::record::{Channel, LogTimestamp, RequestDetails};
use serde::Serialize;
use uuid::Uuid;

/// The version stamp of every access log record.
pub const VERSION: &str = "accesslog/v1beta2";

/// The envelope common to every access record (the legacy `AccessRecord.Base`
/// fields), followed by the method-specific payload flattened into the same
/// JSON object.
#[derive(Clone, Debug, Serialize)]
pub struct AccessRecord {
    pub version: &'static str,
    pub id: Uuid,
    pub channel: Channel,
    pub ts: LogTimestamp,
    #[serde(flatten)]
    pub payload: Payload,
}

impl AccessRecord {
    /// A record of the current moment with a fresh id.
    pub fn new(channel: Channel, payload: Payload) -> Self {
        Self {
            version: VERSION,
            id: Uuid::new_v4(),
            channel,
            ts: LogTimestamp::now(),
            payload,
        }
    }
}

/// The method-specific part of a record. The variant name is serialized as
/// the `method` field, so variants that share a shape (e.g. `GetBalance` and
/// `SubscribeBalance`) are distinct entries over the same detail struct.
/// Every method except `Describe` is chain-scoped (the legacy `ChainBase`).
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "method")]
pub enum Payload {
    NativeCall(OnBlockchain<NativeCall>),
    NativeSubscribe(OnBlockchain<NativeSubscribe>),
    SubscribeHead(OnBlockchain<SubscribeHead>),
    SubscribeBalance(OnBlockchain<Balance>),
    GetBalance(OnBlockchain<Balance>),
    SubscribeAddressAllowance(OnBlockchain<Allowance>),
    GetAddressAllowance(OnBlockchain<Allowance>),
    SubscribeTxStatus(OnBlockchain<TxStatus>),
    Describe(Describe),
    Status(OnBlockchain<Status>),
    EstimateFee(OnBlockchain<EstimateFee>),
}

/// A chain-scoped payload — the legacy `ChainBase` records: the blockchain
/// name followed by the method detail fields.
#[derive(Clone, Debug, Serialize)]
pub struct OnBlockchain<T> {
    pub blockchain: &'static str,
    #[serde(flatten)]
    pub value: T,
}

impl<T> OnBlockchain<T> {
    /// `None` (an unknown or unparseable chain) is reported as `UNSPECIFIED`,
    /// like the legacy records.
    pub fn new(chain: Option<&TargetBlockchain>, value: T) -> Self {
        Self {
            blockchain: chain.map(|c| c.legacy_name()).unwrap_or("UNSPECIFIED"),
            value,
        }
    }
}

/// One reply of a `NativeCall` request, over any channel.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCall {
    pub request: RequestDetails,
    pub total: usize,
    pub index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quorum: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_availability: Option<String>,
    pub succeed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_error: Option<i32>,
    pub payload_size_bytes: u64,
    pub native_call: NativeCallItemDetails,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// The requested call item a [`NativeCall`] record replies to. Unlike the
/// records themselves, a `null` `requestParams` is written explicitly — the
/// legacy class had no null-suppression here.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCallItemDetails {
    pub method: String,
    pub id: u32,
    pub payload_size_bytes: u64,
    pub nonce: u64,
    pub request_params: Option<String>,
}

/// One message pushed to a `NativeSubscribe` (or WebSocket `eth_subscribe`)
/// subscription.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSubscribe {
    pub request: RequestDetails,
    pub payload_size_bytes: u64,
    pub native_subscribe: NativeSubscribeItemDetails,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_body: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSubscribeItemDetails {
    pub method: String,
    pub payload_size_bytes: u64,
}

/// One head pushed to a `SubscribeHead` subscription.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeHead {
    pub request: RequestDetails,
    pub index: usize,
}

/// One balance pushed for a `SubscribeBalance` / `GetBalance` request.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    pub request: RequestDetails,
    pub balance_request: BalanceRequest,
    pub address_balance: AddressBalance,
    pub index: usize,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceRequest {
    pub asset: String,
    pub address_type: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressBalance {
    pub asset: String,
    pub address: String,
}

/// One allowance pushed for a `SubscribeAddressAllowance` /
/// `GetAddressAllowance` request.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Allowance {
    pub request: RequestDetails,
    pub address_allowance_request: AddressAllowanceRequest,
    pub address_allowance: AddressAllowance,
    pub index: usize,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressAllowanceRequest {
    pub address_type: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressAllowance {
    pub address: String,
}

/// One status pushed for a `SubscribeTxStatus` request.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TxStatus {
    pub request: RequestDetails,
    pub tx_status_request: TxStatusRequest,
    pub tx_status: TxStatusResponse,
    pub index: usize,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TxStatusRequest {
    pub tx_id: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TxStatusResponse {
    pub confirmations: u32,
}

/// The reply to a `Describe` request. Not chain-specific.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Describe {
    pub request: RequestDetails,
}

/// One status pushed for a `SubscribeStatus` request.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub request: RequestDetails,
}

/// The reply to an `EstimateFee` request.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EstimateFee {
    pub request: RequestDetails,
    pub estimate_fee: EstimateFeeDetails,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EstimateFeeDetails {
    pub mode: String,
    pub blocks: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_details() -> RequestDetails {
        RequestDetails {
            id: "429aa631-434b-4bdc-a58b-a61b935d2413".parse().unwrap(),
            start: LogTimestamp("2025-08-21T23:10:31.902212Z".parse().unwrap()),
            remote: Some(crate::logs::record::Remote {
                ips: vec!["127.0.0.1".to_string()],
                ip: "127.0.0.1".to_string(),
                user_agent: "Apache-HttpClient/4.5.12 (Java/21.0.2)".to_string(),
            }),
        }
    }

    fn record(channel: Channel, payload: Payload) -> AccessRecord {
        AccessRecord {
            version: VERSION,
            id: "fdcfa4a9-47ba-4cd3-8058-6db207dbe026".parse().unwrap(),
            channel,
            ts: LogTimestamp("2025-08-21T23:10:31.903307Z".parse().unwrap()),
            payload,
        }
    }

    /// Byte-for-byte comparison against a real record written by the legacy
    /// implementation (from the acceptance-testing sample logs).
    #[test]
    fn native_call_matches_legacy_sample() {
        let record = record(
            Channel::JsonRpc,
            Payload::NativeCall(OnBlockchain {
                blockchain: "ETHEREUM",
                value: NativeCall {
                    request: request_details(),
                    total: 1,
                    index: 0,
                    selector: None,
                    quorum: None,
                    min_availability: None,
                    succeed: false,
                    rpc_error: None,
                    payload_size_bytes: 18,
                    native_call: NativeCallItemDetails {
                        method: "eth_getBlockByNumber".to_string(),
                        id: 0,
                        payload_size_bytes: 18,
                        nonce: 0,
                        request_params: None,
                    },
                    response_body: None,
                    error_message: None,
                    nonce: None,
                    signature: None,
                },
            }),
        );

        let expected = r#"{"version":"accesslog/v1beta2","id":"fdcfa4a9-47ba-4cd3-8058-6db207dbe026","channel":"JSONRPC","ts":"2025-08-21T23:10:31.903307Z","method":"NativeCall","blockchain":"ETHEREUM","request":{"id":"429aa631-434b-4bdc-a58b-a61b935d2413","start":"2025-08-21T23:10:31.902212Z","remote":{"ips":["127.0.0.1"],"ip":"127.0.0.1","userAgent":"Apache-HttpClient/4.5.12 (Java/21.0.2)"}},"total":1,"index":0,"succeed":false,"payloadSizeBytes":18,"nativeCall":{"method":"eth_getBlockByNumber","id":0,"payloadSizeBytes":18,"nonce":0,"requestParams":null}}"#;
        assert_eq!(serde_json::to_string(&record).unwrap(), expected);
    }

    #[test]
    fn subscribe_head_serializes_in_order() {
        let record = record(
            Channel::Dshackle,
            Payload::SubscribeHead(OnBlockchain::new(
                Some(&"ethereum".parse().unwrap()),
                SubscribeHead {
                    request: request_details(),
                    index: 3,
                },
            )),
        );
        let json = serde_json::to_string(&record).unwrap();
        assert!(json.starts_with(
            r#"{"version":"accesslog/v1beta2","id":"fdcfa4a9-47ba-4cd3-8058-6db207dbe026","channel":"DSHACKLE","ts":"2025-08-21T23:10:31.903307Z","method":"SubscribeHead","blockchain":"ETHEREUM","request":{"#
        ));
        assert!(json.ends_with(r#""index":3}"#));
    }

    #[test]
    fn shared_shapes_carry_their_own_method() {
        let balance = OnBlockchain {
            blockchain: "ETHEREUM",
            value: Balance {
                request: request_details(),
                balance_request: BalanceRequest {
                    asset: "ETHER".to_string(),
                    address_type: "ADDRESS_SINGLE".to_string(),
                },
                address_balance: AddressBalance {
                    asset: "ETHER".to_string(),
                    address: "0xabc".to_string(),
                },
                index: 0,
            },
        };
        let subscribe = record(Channel::Dshackle, Payload::SubscribeBalance(balance.clone()));
        let get = record(Channel::Dshackle, Payload::GetBalance(balance));
        assert!(
            serde_json::to_string(&subscribe)
                .unwrap()
                .contains(r#""method":"SubscribeBalance""#)
        );
        assert!(
            serde_json::to_string(&get)
                .unwrap()
                .contains(r#""method":"GetBalance""#)
        );
    }

    #[test]
    fn describe_has_no_blockchain() {
        let record = record(
            Channel::Dshackle,
            Payload::Describe(Describe {
                request: request_details(),
            }),
        );
        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains(r#""method":"Describe","request":{"#));
        assert!(!json.contains("blockchain"));
    }
}
