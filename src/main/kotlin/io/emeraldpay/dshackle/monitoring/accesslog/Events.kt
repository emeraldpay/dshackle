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
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*

class Events {

    companion object {
        private val log = LoggerFactory.getLogger(Events::class.java)
    }

    abstract class Base(
            val id: UUID
    ) {
        val ts = Instant.now()
    }

    abstract class ChainBase(
            val blockchain: Chain, val method: String, id: UUID
    ) : Base(id)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class SubscribeHead(
            blockchain: Chain, id: UUID,
            // initial request details
            val request: StreamRequestDetails,
            // index of the current response
            val index: Int
    ) : ChainBase(blockchain, "SubscribeHead", id)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class SubscribeBalance(
            blockchain: Chain, id: UUID, subscribe: Boolean,
            // initial request details
            val request: StreamRequestDetails,
            val balanceRequest: BalanceRequest,
            val addressBalance: AddressBalance,
            // index of the current response
            val index: Int
    ) : ChainBase(blockchain, if (subscribe) "SubscribeBalance" else "GetBalance", id)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class NativeCall(
            blockchain: Chain, id: UUID,

            // info about the initial request, that may include several native calls
            val request: StreamRequestDetails,
            // total native calls passes within the initial request
            val total: Int,
            // index of the call specific for the current response
            val index: Int,
            val selector: String? = null,
            val quorum: Long? = null,
            val minAvailability: String? = null,

            val succeed: Boolean,
            val rpcError: Int? = null,
            val payloadSizeBytes: Long,
            val nativeCall: NativeCallItemDetails
    ) : ChainBase(blockchain, "NativeCall", id)

    data class StreamRequestDetails(
            val id: UUID,
            val start: Instant,
            val remote: Remote
    )

    data class Remote(
            val ips: List<String>,
            val ip: String,
            val userAgent: String
    )

    data class NativeCallItemDetails(
            val method: String,
            val id: Int,
            val payloadSizeBytes: Long
    )

    data class NativeCallReplyDetails(
            val id: Int,
            val succeed: Boolean,
            val replySizeBytes: Long,
            val ts: Instant = Instant.now()
    )

    data class BalanceRequest(
            val asset: String,
            val addressType: String
    )

    data class AddressBalance(
            val asset: String,
            val address: String
    )
}