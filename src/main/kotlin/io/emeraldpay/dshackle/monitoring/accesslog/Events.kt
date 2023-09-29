/**
 * Copyright (c) 2021 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.monitoring.accesslog

import com.fasterxml.jackson.annotation.JsonInclude
import io.emeraldpay.dshackle.Chain
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID

class Events {

    companion object {
        private val log = LoggerFactory.getLogger(Events::class.java)
    }

    enum class Channel {
        GRPC, JSONRPC, WSJSONRPC
    }

    abstract class Base(
        val id: UUID,
        val method: String,
        val channel: Channel,
    ) {
        val version = "accesslog/v1beta"
        var ts = Instant.now()
    }

    abstract class ChainBase(
        val blockchain: Chain,
        method: String,
        id: UUID,
        channel: Channel,
    ) : Base(id, method, channel)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class SubscribeHead(
        blockchain: Chain,
        id: UUID,
        // initial request details
        val request: StreamRequestDetails,
        // index of the current response
        val index: Int,
    ) : ChainBase(blockchain, "SubscribeHead", id, Channel.GRPC)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class SubscribeBalance(
        blockchain: Chain,
        id: UUID,
        subscribe: Boolean,
        // initial request details
        val request: StreamRequestDetails,
        val balanceRequest: BalanceRequest,
        val addressBalance: AddressBalance,
        // index of the current response
        val index: Int,
    ) : ChainBase(blockchain, if (subscribe) "SubscribeBalance" else "GetBalance", id, Channel.GRPC)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class TxStatus(
        blockchain: Chain,
        id: UUID,
        val request: StreamRequestDetails,
        val txStatusRequest: TxStatusRequest,
        val txStatus: TxStatusResponse,
        // index of the current response
        val index: Int,
    ) : ChainBase(blockchain, "SubscribeTxStatus", id, Channel.GRPC)

    data class TxStatusRequest(
        val txId: String,
    )

    data class TxStatusResponse(
        val confirmations: Int,
    )

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class NativeCall(
        blockchain: Chain,
        id: UUID,
        channel: Channel,

        // info about the initial request, that may include several native calls
        val request: StreamRequestDetails,
        // total native calls passes within the initial request
        val total: Int,
        // index of the call specific for the current response
        val index: Int,
        val selector: String? = null,
        val quorum: Long? = null,
        val minAvailability: String? = null,
        val latency: Long,

        val succeed: Boolean,
        val rpcError: Int? = null,
        val payloadSizeBytes: Long,
        val nativeCall: NativeCallItemDetails,
        val responseBody: String? = null,
        val errorMessage: String? = null,
        val nonce: Long? = null,
        val signature: String? = null,
    ) : ChainBase(blockchain, "NativeCall", id, channel)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class NativeSubscribe(
        blockchain: Chain,
        id: UUID,
        channel: Channel,

        // info about the initial request, that may include several native calls
        val request: StreamRequestDetails,
        val payloadSizeBytes: Long,
        val nativeSubscribe: NativeSubscribeItemDetails,
        val responseBody: String? = null,
    ) : ChainBase(blockchain, "NativeSubscribe", id, channel)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class Describe(
        id: UUID,
        val request: StreamRequestDetails,
    ) : Base(id, "Describe", Channel.GRPC)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class Status(
        blockchain: Chain,
        id: UUID,
        val request: StreamRequestDetails,
    ) : ChainBase(blockchain, "Status", id, Channel.GRPC)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class EstimateFee(
        blockchain: Chain,
        id: UUID,
        val request: StreamRequestDetails,
        val estimateFee: EstimateFeeDetails,
    ) : ChainBase(blockchain, "EstimateFee", id, Channel.GRPC)

    data class StreamRequestDetails(
        val id: UUID,
        val start: Instant,
        val remote: Remote,
    )

    data class Remote(
        val ips: List<String>,
        val ip: String,
        val userAgent: String,
    )

    data class NativeCallItemDetails(
        val method: String,
        val id: Int,
        val payloadSizeBytes: Long,
        val nonce: Long,
        val requestParams: String? = null,
    )

    data class NativeCallReplyDetails(
        val id: Int,
        val succeed: Boolean,
        val replySizeBytes: Long,
        val ts: Instant = Instant.now(),
    )

    data class NativeSubscribeItemDetails(
        val method: String,
        val payloadSizeBytes: Long,
    )

    data class NativeSubscribeReplyDetails(
        val replySizeBytes: Long,
        val ts: Instant = Instant.now(),
    )

    data class BalanceRequest(
        val asset: String,
        val addressType: String,
    )

    data class AddressBalance(
        val asset: String,
        val address: String,
    )

    data class EstimateFeeDetails(
        val mode: String,
        val blocks: Int,
    )
}
