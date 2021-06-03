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
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*
import kotlin.collections.ArrayList

class Events {

    companion object {
        private val log = LoggerFactory.getLogger(Events::class.java)
    }

    abstract class Base(
            val method: String,
            val id: UUID
    ) {
        val ts = Instant.now()
    }

    abstract class ChainBase(
            val blockchain: Chain, method: String, id: UUID
    ) : Base(method, id) {

    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class NativeCall(
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

            blockchain: Chain, method: String, id: UUID
    ) : ChainBase(blockchain, method, id) {

    }

    data class StreamRequestDetails(
            val id: UUID,
            val start: Instant
    )

    data class Remote(
            val ips: List<String>,
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

    class NativeCallBuilder() {

        private val requestDetails = StreamRequestDetails(
                UUID.randomUUID(),
                Instant.now()
        )

        var chain: Int = Chain.UNSPECIFIED.id
        val items = ArrayList<NativeCallItemDetails>()
        val replies = HashMap<Int, NativeCallReplyDetails>()

        fun withChain(chain: Int): NativeCallBuilder {
            this.chain = chain
            return this
        }

        fun onItem(item: BlockchainOuterClass.NativeCallItem): NativeCallBuilder {
            this.items.add(
                    NativeCallItemDetails(
                            item.method,
                            item.id,
                            item.payload.size().toLong()
                    )
            )
            return this
        }

        fun onItemReply(reply: BlockchainOuterClass.NativeCallReplyItem): NativeCallBuilder {
            this.replies[reply.id] = NativeCallReplyDetails(
                    reply.id,
                    reply.succeed,
                    reply.payload?.size()?.toLong() ?: 0L
            )
            return this
        }

        fun build(): List<NativeCall> {
            val blockchain = Chain.byId(this.chain)
            return items.mapIndexed { index, item ->
                val reply = replies[item.id]
                NativeCall(
                        request = requestDetails,
                        total = items.size,
                        index = index,
                        succeed = reply?.succeed ?: false,
                        blockchain = blockchain,
                        method = item.method,
                        payloadSizeBytes = item.payloadSizeBytes,
                        id = UUID.randomUUID()
                )
            }
        }
    }
}