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

class EventsBuilder {

    companion object {
        private val log = LoggerFactory.getLogger(EventsBuilder::class.java)
    }

    abstract class Base<T>() {
        companion object {
            private val remoteIpKeys = listOf(
                    Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER),
                    Metadata.Key.of("x-forwarded-for", Metadata.ASCII_STRING_MARSHALLER)
            )
            private val invalidCharacters = Regex("[\n\t]+")
        }

        var requestDetails = Events.StreamRequestDetails(
                UUID.randomUUID(),
                Instant.now(),
                Events.Remote(emptyList(), "", "")
        )

        var chainId: Int = Chain.UNSPECIFIED.id
        var chain = Chain.UNSPECIFIED

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

        abstract protected fun getT(): T

        fun start(metadata: Metadata, attributes: Attributes): T {
            val userAgent = metadata.get(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER))
                    ?.let(this@Base::clean)
                    ?: ""
            val ips = ArrayList<InetAddress>()
            remoteIpKeys.forEach { key ->
                metadata.get(key)?.let {
                    it.trim().ifEmpty { null }
                            ?.let(this@Base::toInetAddress)
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
                    .copy(remote = Events.Remote(
                            ips = ips.map { it.hostAddress },
                            ip = ip,
                            userAgent = userAgent
                    ))
            return getT()
        }

        fun withChain(chain: Int): T {
            this.chainId = chain
            this.chain = Chain.byId(chainId)
            return getT()
        }
    }

    class SubscribeHead() : Base<SubscribeHead>() {
        private var index = 0

        override fun getT(): SubscribeHead {
            return this
        }

        fun onReply(resp: BlockchainOuterClass.ChainHead): Events.SubscribeHead {
            return Events.SubscribeHead(
                    chain, UUID.randomUUID(), requestDetails, index++
            )
        }
    }

    class SubscribeBalance(val subscribe: Boolean) : Base<SubscribeBalance>() {
        private var index = 0
        private var balanceRequest: Events.BalanceRequest? = null

        override fun getT(): SubscribeBalance {
            return this
        }

        fun withRequest(req: BlockchainOuterClass.BalanceRequest): SubscribeBalance {
            balanceRequest = Events.BalanceRequest(
                    req.asset.code.toUpperCase(),
                    req.address.addrTypeCase.name
            )
            return this
        }

        fun onReply(resp: BlockchainOuterClass.AddressBalance): Events.SubscribeBalance {
            if (balanceRequest == null) {
                throw IllegalStateException("Request is not initialized")
            }
            val addressBalance = Events.AddressBalance(resp.asset.code, resp.address.address)
            val chain = Chain.byId(resp.asset.chain.number)
            return Events.SubscribeBalance(
                    chain, UUID.randomUUID(), subscribe, requestDetails, balanceRequest!!, addressBalance, index++
            )
        }
    }

    class NativeCall : Base<NativeCall>() {
        val items = ArrayList<Events.NativeCallItemDetails>()
        val replies = HashMap<Int, Events.NativeCallReplyDetails>()

        override fun getT(): NativeCall {
            return this
        }

        fun onItem(item: BlockchainOuterClass.NativeCallItem): NativeCall {
            this.items.add(
                    Events.NativeCallItemDetails(
                            item.method,
                            item.id,
                            item.payload.size().toLong()
                    )
            )
            return this
        }

        fun onItemReply(reply: BlockchainOuterClass.NativeCallReplyItem): NativeCall {
            this.replies[reply.id] = Events.NativeCallReplyDetails(
                    reply.id,
                    reply.succeed,
                    reply.payload?.size()?.toLong() ?: 0L
            )
            return this
        }

        fun build(): List<Events.NativeCall> {
            return items.mapIndexed { index, item ->
                val reply = replies[item.id]
                Events.NativeCall(
                        request = requestDetails,
                        total = items.size,
                        index = index,
                        succeed = reply?.succeed ?: false,
                        blockchain = chain,
                        nativeCall = item,
                        payloadSizeBytes = item.payloadSizeBytes,
                        id = UUID.randomUUID()
                )
            }
        }
    }
}