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
import io.grpc.Attributes
import io.grpc.Grpc
import io.grpc.Metadata
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.InetSocketAddress
import java.time.Instant
import java.util.*

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

    class NativeCallBuilder() {

        companion object {
            private val remoteIpKeys = listOf(
                    Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER),
                    Metadata.Key.of("x-forwarded-for", Metadata.ASCII_STRING_MARSHALLER)
            )
            private val invalidCharacters = Regex("[\n\t]+")
        }

        private var requestDetails = StreamRequestDetails(
                UUID.randomUUID(),
                Instant.now(),
                Remote(emptyList(), "", "")
        )

        var chain: Int = Chain.UNSPECIFIED.id
        val items = ArrayList<NativeCallItemDetails>()
        val replies = HashMap<Int, NativeCallReplyDetails>()

        private fun toInetAddress(ip: String): InetAddress? {
            val isIp = Character.digit(ip[0], 16) != -1
            if (!isIp) {
                return null
            }
            return try {
                InetAddress.getByName(ip)
            } catch (t: Throwable) {
                null
            }
        }

        private fun findBestIp(ips: List<InetAddress>): InetAddress? {
            // check if a real remote address is provided, otherwise use any local address
            return ips.sortedWith(kotlin.Comparator { a, b ->
                val aLocal = a.isLoopbackAddress || a.isSiteLocalAddress
                val bLocal = b.isLoopbackAddress || b.isSiteLocalAddress
                when {
                    aLocal && bLocal -> 0
                    aLocal -> 1
                    else -> -1
                }
            }).firstOrNull()
        }

        private fun clean(s: String): String {
            return StringUtils.truncate(s, 128)
                    .replace(invalidCharacters, " ")
                    .trim()
        }

        fun start(metadata: Metadata, attributes: Attributes): NativeCallBuilder {
            val userAgent = metadata.get(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER))
                    ?.let(this@NativeCallBuilder::clean)
                    ?: ""
            val ips = ArrayList<InetAddress>()
            remoteIpKeys.forEach { key ->
                metadata.get(key)?.let {
                    it.trim().ifEmpty { null }
                            ?.let(this@NativeCallBuilder::toInetAddress)
                            ?.let(ips::add)
                }
            }
            attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)?.let { addr ->
                if (addr is InetSocketAddress) {
                    ips.add(addr.address)
                }
            }
            val ip = findBestIp(ips)?.hostAddress ?: ""
            this.requestDetails = this.requestDetails
                    .copy(remote = Remote(
                            ips = ips.map { it.hostAddress },
                            ip = ip,
                            userAgent = userAgent
                    ))
            return this
        }

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