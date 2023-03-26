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
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.config.AccessLogConfig
import io.emeraldpay.dshackle.monitoring.Channel
import io.emeraldpay.dshackle.monitoring.record.AccessRecord
import io.emeraldpay.grpc.Chain
import io.grpc.Attributes
import io.grpc.Grpc
import io.grpc.Metadata
import io.netty.handler.codec.http.HttpHeaders
import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.websocket.WebsocketInbound
import java.net.InetAddress
import java.net.InetSocketAddress
import java.time.Instant
import java.util.Locale
import java.util.UUID

class RecordBuilder {

    companion object {
        private val log = LoggerFactory.getLogger(RecordBuilder::class.java)

        // A reference to the config for the current _running instance_.
        // Initialized by AccessLogWriter
        var accessLogConfig: AccessLogConfig = AccessLogConfig.default()
    }

    interface StartingHttp2Request {
        fun start(metadata: Metadata, attributes: Attributes)
    }

    interface StartingHttp1Request {
        fun start(request: HttpServerRequest)
    }

    interface StartingWsRequest {
        fun start(request: WebsocketInbound)
    }

    interface RequestReply<E, Req, Resp> : StartingHttp2Request {
        fun onRequest(msg: Req)
        fun onReply(msg: Resp): E
    }

    abstract class Base<T>(
        requestId: UUID,
    ) : StartingHttp2Request, StartingHttp1Request, StartingWsRequest {
        companion object {
            private val remoteIpHeaders = listOf(
                "x-real-ip",
                "x-forwarded-for"
            )
            private val remoteIpKeys = listOf(
                Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER),
                Metadata.Key.of("x-forwarded-for", Metadata.ASCII_STRING_MARSHALLER)
            )
            private val invalidCharacters = Regex("[\n\t]+")
        }

        var requestDetails = AccessRecord.RequestDetails(
            requestId,
            Instant.now(),
            AccessRecord.Remote(emptyList(), "", "")
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
            return ips.sortedWith(
                kotlin.Comparator { a, b ->
                    val aLocal = a.isLoopbackAddress || a.isSiteLocalAddress
                    val bLocal = b.isLoopbackAddress || b.isSiteLocalAddress
                    when {
                        aLocal && bLocal -> 0
                        aLocal -> 1
                        else -> -1
                    }
                }
            ).firstOrNull()
        }

        private fun clean(s: String): String {
            return StringUtils.truncate(s, 128)
                .replace(invalidCharacters, " ")
                .trim()
        }

        protected abstract fun getT(): T

        override fun start(metadata: Metadata, attributes: Attributes) {
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
                .copy(
                    remote = AccessRecord.Remote(
                        ips = ips.map { it.hostAddress },
                        ip = ip,
                        userAgent = userAgent
                    )
                )
        }

        override fun start(request: HttpServerRequest) {
            val headers = request.requestHeaders()
            val userAgent = getUserAgent(headers)
            val ips = ArrayList<InetAddress>()
            extractIps(headers, ips)
            request.remoteAddress()?.let { addr ->
                ips.add(addr.address)
            }
            val ip = findBestIp(ips)?.hostAddress ?: ""
            this.requestDetails = this.requestDetails
                .copy(
                    remote = AccessRecord.Remote(
                        ips = ips.map { it.hostAddress },
                        ip = ip,
                        userAgent = userAgent
                    )
                )
        }

        override fun start(request: WebsocketInbound) {
            val headers = request.headers()
            val userAgent = getUserAgent(headers)
            val ips = ArrayList<InetAddress>()
            extractIps(headers, ips)
            // class WebsocketServerOperations, which is an implementation for the Websocket server connection, has a remoteAddress method
            // But the class, and it's parent HttpServerOperations, are both private and cannot be used directly,
            // so we try to access the field via reflection when it's possible
            val remoteAddress: InetSocketAddress? = request.javaClass.methods
                .find { it.name == "remoteAddress" }
                ?.let {
                    if (it.canAccess(request) || it.trySetAccessible()) {
                        it.invoke(request) as InetSocketAddress
                    } else {
                        null
                    }
                }
            remoteAddress?.let { addr ->
                ips.add(addr.address)
            }
            val ip = findBestIp(ips)?.hostAddress ?: ""
            this.requestDetails = this.requestDetails
                .copy(
                    remote = AccessRecord.Remote(
                        ips = ips.map { it.hostAddress },
                        ip = ip,
                        userAgent = userAgent
                    )
                )
        }

        fun getUserAgent(headers: HttpHeaders): String {
            return headers.get("user-agent")
                ?.let(this@Base::clean)
                ?: ""
        }

        fun extractIps(headers: HttpHeaders, ips: MutableList<InetAddress>) {
            remoteIpHeaders.forEach { key ->
                headers.get(key)?.let {
                    it.trim().ifEmpty { null }
                        ?.let(this@Base::toInetAddress)
                        ?.let(ips::add)
                }
            }
        }

        fun withChain(chain: Int): T {
            this.chainId = chain
            this.chain = Chain.byId(chainId)
            return getT()
        }
    }

    class SubscribeHead(requestId: UUID) :
        Base<SubscribeHead>(requestId),
        RequestReply<AccessRecord.SubscribeHead, Common.Chain, BlockchainOuterClass.ChainHead> {

        private var index = 0

        override fun getT(): SubscribeHead {
            return this
        }

        override fun onRequest(msg: Common.Chain) {
            withChain(msg.type.number)
        }

        override fun onReply(msg: BlockchainOuterClass.ChainHead): AccessRecord.SubscribeHead {
            return AccessRecord.SubscribeHead(
                chain, UUID.randomUUID(), requestDetails, index++
            )
        }
    }

    class SubscribeBalance(val subscribe: Boolean, requestId: UUID) :
        Base<SubscribeBalance>(requestId),
        RequestReply<AccessRecord.SubscribeBalance, BlockchainOuterClass.BalanceRequest, BlockchainOuterClass.AddressBalance> {

        private var index = 0
        private var balanceRequest: AccessRecord.BalanceRequest? = null

        override fun getT(): SubscribeBalance {
            return this
        }

        override fun onRequest(msg: BlockchainOuterClass.BalanceRequest) {
            balanceRequest = AccessRecord.BalanceRequest(
                msg.asset.code.uppercase(Locale.getDefault()),
                msg.address.addrTypeCase.name
            )
        }

        override fun onReply(msg: BlockchainOuterClass.AddressBalance): AccessRecord.SubscribeBalance {
            if (balanceRequest == null) {
                throw IllegalStateException("Request is not initialized")
            }
            val addressBalance = AccessRecord.AddressBalance(msg.asset.code, msg.address.address)
            val chain = Chain.byId(msg.asset.chain.number)
            return AccessRecord.SubscribeBalance(
                chain, UUID.randomUUID(), subscribe, requestDetails, balanceRequest!!, addressBalance, index++
            )
        }
    }

    class TxStatus(requestId: UUID) :
        Base<TxStatus>(requestId),
        RequestReply<AccessRecord.TxStatus, BlockchainOuterClass.TxStatusRequest, BlockchainOuterClass.TxStatus> {
        private var index = 0
        private var txStatusRequest: AccessRecord.TxStatusRequest? = null

        override fun onRequest(msg: BlockchainOuterClass.TxStatusRequest) {
            this.txStatusRequest = AccessRecord.TxStatusRequest(msg.txId)
            withChain(msg.chainValue)
        }

        override fun onReply(msg: BlockchainOuterClass.TxStatus): AccessRecord.TxStatus {
            return AccessRecord.TxStatus(
                chain, UUID.randomUUID(), requestDetails, txStatusRequest!!,
                AccessRecord.TxStatusResponse(msg.confirmations),
                index++
            )
        }

        override fun getT(): TxStatus {
            return this
        }
    }

    class NativeCall(requestId: UUID) :
        Base<NativeCall>(requestId),
        RequestReply<AccessRecord.NativeCall, BlockchainOuterClass.NativeCallRequest, BlockchainOuterClass.NativeCallReplyItem> {
        val items = ArrayList<AccessRecord.NativeCallItemDetails>()
        private var index = 0

        override fun getT(): NativeCall {
            return this
        }

        override fun onRequest(msg: BlockchainOuterClass.NativeCallRequest) {
            withChain(msg.chain.number)
            msg.itemsList.forEach { item ->
                this.items.add(
                    AccessRecord.NativeCallItemDetails(
                        item.method,
                        item.id,
                        item.payload.size().toLong(),
                        item.nonce,
                        if (accessLogConfig.includeMessages) {
                            if (item.payload != null && !item.payload.isEmpty && item.payload.isValidUtf8) item.payload.toStringUtf8() else ""
                        } else null
                    )
                )
            }
        }

        override fun onReply(msg: BlockchainOuterClass.NativeCallReplyItem): AccessRecord.NativeCall {
            val item = items.find { it.id == msg.id }!!
            return AccessRecord.NativeCall(
                request = requestDetails,
                total = items.size,
                index = index++,
                succeed = msg.succeed,
                blockchain = chain,
                nativeCall = item,
                payloadSizeBytes = item.payloadSizeBytes,
                id = UUID.randomUUID(),
                channel = Channel.DSHACKLE,
                responseBody = if (accessLogConfig.includeMessages) {
                    if (msg.payload != null && !msg.payload.isEmpty && msg.payload.isValidUtf8) msg.payload.toStringUtf8() else ""
                } else null,
                errorMessage = if (accessLogConfig.includeMessages) msg.errorMessage else null,
                signature = Hex.encodeHexString(msg.signature.signature.toByteArray()),
                nonce = msg.signature.nonce
            )
        }

        fun onReply(
            reply: io.emeraldpay.dshackle.rpc.NativeCall.CallResult,
            channel: Channel
        ): AccessRecord.NativeCall {
            val item = items.find { it.id == reply.id }!!
            return AccessRecord.NativeCall(
                request = requestDetails,
                total = items.size,
                index = index++,
                succeed = !reply.isError(),
                blockchain = chain,
                nativeCall = item,
                payloadSizeBytes = item.payloadSizeBytes,
                id = UUID.randomUUID(),
                channel = channel,
                responseBody = if (accessLogConfig.includeMessages) (reply.result?.let { String(it) } ?: "") else null,
                errorMessage = if (accessLogConfig.includeMessages) {
                    reply.error?.let {
                        it.upstreamError?.message ?: it.message
                    } ?: ""
                } else null
            )
        }
    }

    class NativeSubscribe(
        val channel: Channel,
        requestId: UUID,
    ) :
        Base<NativeSubscribe>(requestId),
        RequestReply<AccessRecord.NativeSubscribe, BlockchainOuterClass.NativeSubscribeRequest, BlockchainOuterClass.NativeSubscribeReplyItem> {
        var item: AccessRecord.NativeSubscribeItemDetails? = null
        val replies = HashMap<Int, AccessRecord.NativeSubscribeReplyDetails>()

        override fun getT(): NativeSubscribe {
            return this
        }

        override fun onRequest(msg: BlockchainOuterClass.NativeSubscribeRequest) {
            withChain(msg.chain.number)
            this.item = AccessRecord.NativeSubscribeItemDetails(
                msg.method,
                msg.payload.size().toLong()
            )
        }

        override fun onReply(msg: BlockchainOuterClass.NativeSubscribeReplyItem): AccessRecord.NativeSubscribe {
            return AccessRecord.NativeSubscribe(
                request = requestDetails,
                blockchain = chain,
                nativeSubscribe = item!!,
                payloadSizeBytes = msg.payload?.size()?.toLong() ?: 0L,
                id = UUID.randomUUID(),
                channel = Channel.DSHACKLE,
                responseBody = if (accessLogConfig.includeMessages) (msg.payload?.toStringUtf8() ?: "") else null,
            )
        }
    }

    class NativeSubscribeHttp(
        val channel: Channel,
        chain: Chain,
        requestId: UUID,
    ) :
        Base<NativeSubscribeHttp>(requestId),
        RequestReply<AccessRecord.NativeSubscribe, Pair<String, ByteArray?>, Long> {
        var item: AccessRecord.NativeSubscribeItemDetails? = null
        val replies = HashMap<Int, AccessRecord.NativeSubscribeReplyDetails>()

        init {
            withChain(chain.id)
        }

        override fun getT(): NativeSubscribeHttp {
            return this
        }

        override fun onRequest(msg: Pair<String, ByteArray?>) {
            this.item = AccessRecord.NativeSubscribeItemDetails(
                msg.first,
                msg.second?.size?.toLong() ?: 0L
            )
        }

        override fun onReply(msg: Long): AccessRecord.NativeSubscribe {
            return AccessRecord.NativeSubscribe(
                request = requestDetails,
                blockchain = chain,
                nativeSubscribe = item!!,
                payloadSizeBytes = msg,
                id = UUID.randomUUID(),
                channel = channel
            )
        }
    }

    class Describe(requestId: UUID) :
        Base<Describe>(requestId),
        RequestReply<AccessRecord.Describe, BlockchainOuterClass.DescribeRequest, BlockchainOuterClass.DescribeResponse> {

        override fun getT(): Describe {
            return this
        }

        override fun onRequest(msg: BlockchainOuterClass.DescribeRequest) {
        }

        override fun onReply(msg: BlockchainOuterClass.DescribeResponse): AccessRecord.Describe {
            return AccessRecord.Describe(
                id = UUID.randomUUID(),
                request = requestDetails
            )
        }
    }

    class Status(requestId: UUID) :
        Base<Status>(requestId),
        RequestReply<AccessRecord.Status, BlockchainOuterClass.StatusRequest, BlockchainOuterClass.ChainStatus> {
        override fun getT(): Status {
            return this
        }

        override fun onRequest(msg: BlockchainOuterClass.StatusRequest) {
        }

        override fun onReply(msg: BlockchainOuterClass.ChainStatus): AccessRecord.Status {
            val chain = Chain.byId(msg.chainValue)
            return AccessRecord.Status(
                blockchain = chain,
                request = requestDetails,
                id = UUID.randomUUID()
            )
        }
    }

    class EstimateFee(requestId: UUID) :
        Base<EstimateFee>(requestId),
        RequestReply<AccessRecord.EstimateFee, BlockchainOuterClass.EstimateFeeRequest, BlockchainOuterClass.EstimateFeeResponse> {

        private var mode: String = "UNKNOWN"
        private var blocks: Int = 0

        override fun getT(): EstimateFee {
            return this
        }

        override fun onRequest(msg: BlockchainOuterClass.EstimateFeeRequest) {
            this.chain = Chain.byId(msg.chain.number)
            this.mode = msg.mode.name
            this.blocks = msg.blocks
        }

        override fun onReply(msg: BlockchainOuterClass.EstimateFeeResponse): AccessRecord.EstimateFee {
            return AccessRecord.EstimateFee(
                blockchain = chain,
                request = requestDetails,
                id = UUID.randomUUID(),
                estimateFee = AccessRecord.EstimateFeeDetails(
                    mode = mode,
                    blocks = blocks
                )
            )
        }
    }
}
