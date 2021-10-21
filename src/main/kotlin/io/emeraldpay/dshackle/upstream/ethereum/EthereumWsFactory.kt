/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.ResponseWSParser
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.HttpHeaderNames
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.BackOffExecution
import org.springframework.util.backoff.ExponentialBackOff
import org.springframework.util.backoff.FixedBackOff
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import reactor.netty.http.client.HttpClient
import reactor.netty.http.client.WebsocketClientSpec
import reactor.netty.http.websocket.WebsocketInbound
import reactor.netty.http.websocket.WebsocketOutbound
import reactor.retry.Repeat
import reactor.util.function.Tuples
import java.net.URI
import java.time.Duration
import java.util.Base64
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class EthereumWsFactory(
    private val uri: URI,
    private val origin: URI
) {

    var basicAuth: AuthConfig.ClientBasicAuth? = null
    var config: UpstreamsConfig.WsEndpoint? = null

    fun create(upstream: DefaultUpstream?, validator: EthereumUpstreamValidator?, rpcMetrics: RpcMetrics?): EthereumWs {
        return EthereumWs(uri, origin, basicAuth, rpcMetrics, upstream, validator).also { ws ->
            config?.frameSize?.let {
                ws.frameSize = it
            }
            config?.msgSize?.let {
                ws.msgSizeLimit = it
            }
        }
    }

    class EthereumWs(
        private val uri: URI,
        private val origin: URI,
        private val basicAuth: AuthConfig.ClientBasicAuth?,
        private val rpcMetrics: RpcMetrics?,
        private val upstream: DefaultUpstream?,
        private val validator: EthereumUpstreamValidator?
    ) : AutoCloseable {

        companion object {
            private val log = LoggerFactory.getLogger(EthereumWs::class.java)

            private const val IDS_START = 100
            private const val START_REQUEST =
                "{\"jsonrpc\":\"2.0\", \"method\":\"eth_subscribe\", \"id\":\"blocks\", \"params\":[\"newHeads\"]}"

            // WebSocket Frame limit.
            // Default is 65_536, but Geth responds with larger frames,
            // and connection gets dropped with:
            // > io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException: Max frame length of 65536 has been exceeded
            // It's unclear what is a right limit here, but 5mb seems to be working (1mb wasn't always working)
            private const val DEFAULT_FRAME_SIZE = 5 * 1024 * 1024

            // The max size from multiple frames that may represent a single message
            // Accept up to 15Mb messages, because Geth is using 15mb, though it's not clear what it limits
            private const val DEFAULT_MSG_SIZE = 15 * 1024 * 1024
        }

        var frameSize: Int = DEFAULT_FRAME_SIZE
        var msgSizeLimit: Int = DEFAULT_MSG_SIZE

        private var reconnectBackoff: BackOff = ExponentialBackOff().also {
            it.initialInterval = Duration.ofMillis(100).toMillis()
            it.maxInterval = Duration.ofMinutes(1).toMillis()
        }
        private var currentBackOff = reconnectBackoff.start()

        private val parser = ResponseWSParser()

        private val blocks = Sinks
            .many()
            .multicast()
            .directBestEffort<BlockContainer>()
        private var rpcSend = Sinks
            .many()
            .unicast()
            .onBackpressureBuffer<JsonRpcRequest>()
        private val rpcReceive = Sinks
            .many()
            .multicast()
            .directBestEffort<JsonRpcResponse>()
        private val sendIdSeq = AtomicInteger(IDS_START)
        private val sendExecutor = Executors.newSingleThreadExecutor()
        private var keepConnection = true
        private var connection: Disposable? = null
        private val reconnecting = AtomicBoolean(false)

        fun setReconnectIntervalSeconds(value: Long) {
            reconnectBackoff = FixedBackOff(value * 1000, FixedBackOff.UNLIMITED_ATTEMPTS)
            currentBackOff = reconnectBackoff.start()
        }

        fun connect() {
            keepConnection = true
            connectInternal()
        }

        private fun tryReconnectLater() {
            if (!keepConnection) {
                return
            }
            val alreadyReconnecting = reconnecting.getAndSet(true)
            if (alreadyReconnecting) {
                return
            }
            // rpcSend is already CANCELLED, since the subscription owned by the previous connection is gone
            // so we need to create a new Sink. Emit Complete is probably useless, and just in case
            rpcSend.tryEmitComplete()
            rpcSend = Sinks
                .many()
                .unicast()
                .onBackpressureBuffer<JsonRpcRequest>()
            val retryInterval = currentBackOff.nextBackOff()
            if (retryInterval == BackOffExecution.STOP) {
                log.warn("Reconnect backoff exhausted. Permanently closing the connection")
                return
            }
            log.info("Reconnect to $uri in ${retryInterval}ms...")
            Global.control.schedule(
                {
                    reconnecting.set(false)
                    connectInternal()
                },
                retryInterval, TimeUnit.MILLISECONDS
            )
        }

        private fun connectInternal() {
            log.info("Connecting to WebSocket: $uri")
            connection?.dispose()
            connection = HttpClient.create()
                .doOnDisconnected {
                    log.info("Disconnected from $uri")
                    // mark upstream as UNAVAIL
                    upstream?.setStatus(UpstreamAvailability.UNAVAILABLE)
                    if (keepConnection) {
                        tryReconnectLater()
                    }
                }
                .doOnError(
                    { _, t ->
                        log.warn("Failed to connect to $uri. Error: ${t.message}")
                        // going to try to reconnect later
                        tryReconnectLater()
                    },
                    { _, _ -> }
                )
                .headers { headers ->
                    headers.add(HttpHeaderNames.ORIGIN, origin)
                    basicAuth?.let { auth ->
                        val tmp: String = auth.username + ":" + auth.password
                        val base64password = Base64.getEncoder().encodeToString(tmp.toByteArray())
                        headers.add(HttpHeaderNames.AUTHORIZATION, "Basic $base64password")
                    }
                }
                .let {
                    if (uri.scheme == "wss") it.secure() else it
                }
                .websocket(
                    WebsocketClientSpec.builder()
                        .handlePing(true)
                        .compress(false)
                        .maxFramePayloadLength(frameSize)
                        .build()
                )
                .uri(uri)
                .handle { inbound, outbound ->
                    handle(inbound, outbound)
                }
                .onErrorResume { t ->
                    log.debug("Dropping WS connection to $uri. Error: ${t.message}")
                    Mono.empty<Void>()
                }
                .subscribe()
        }

        fun handle(inbound: WebsocketInbound, outbound: WebsocketOutbound): Publisher<Void> {
            // restart backoff after connection
            currentBackOff = reconnectBackoff.start()

            // validate the connection, it can also be UNAVAIL if market as such after disconnect
            validator?.validate()

            val consumer = inbound
                .aggregateFrames(msgSizeLimit)
                .receiveFrames()
                .map { ByteBufInputStream(it.content()).readAllBytes() }
                .flatMap {
                    try {
                        val msg = parser.parse(it)
                        if (msg.type == ResponseWSParser.Type.SUBSCRIPTION) {
                            onSubscription(msg)
                        } else {
                            onRpc(msg)
                        }
                    } catch (t: Throwable) {
                        log.warn("Failed to process WS message. ${t.javaClass}: ${t.message}")
                        Mono.empty()
                    }
                }
                .onErrorResume { t ->
                    log.warn("Connection dropped to $uri. Error: ${t.message}", t)
                    // going to try to reconnect later
                    tryReconnectLater()
                    // completes current outbound flow
                    Mono.empty()
                }

            val start = Mono.just(START_REQUEST).map {
                Unpooled.wrappedBuffer(it.toByteArray())
            }
            val calls = rpcSend
                .asFlux()
                .map {
                    Unpooled.wrappedBuffer(Global.objectMapper.writeValueAsBytes(it))
                }

            return outbound.send(
                Flux.merge(
                    start,
                    calls.subscribeOn(Schedulers.boundedElastic()),
                    consumer.then(Mono.empty<ByteBuf>()).subscribeOn(Schedulers.boundedElastic())
                )
            )
        }

        fun onRpc(msg: ResponseWSParser.WsResponse): Mono<Void> {
            return if (msg.id.isNumber()) {
                val resp = JsonRpcResponse(
                    msg.value, msg.error, msg.id
                )
                Mono.fromCallable {
                    val status = rpcReceive.tryEmitNext(resp)
                    if (status.isFailure) {
                        log.warn("Failed to proceed with a RPC message: $status")
                    }
                }.then()
            } else {
                // it's a response to the newHeads subscription, just ignore it
                Mono.empty<Void>()
            }
        }

        fun onSubscription(msg: ResponseWSParser.WsResponse): Mono<Void> {
            if (msg.error != null) {
                return Mono.error(IllegalStateException("Received error from WS upstream: ${msg.error.message}"))
            }
            // we always expect an answer to the `newHeads`, since we are not initiating any other subscriptions
            return Mono.fromCallable {
                Global.objectMapper.readValue(msg.value, BlockJson::class.java) as BlockJson<TransactionRefJson>
            }.flatMap { onNewHeads(it) }.then()
        }

        fun onNewHeads(block: BlockJson<TransactionRefJson>): Mono<Void> {
            // newHeads returns incomplete blocks, i.e. without some fields and without transaction hashes,
            // so we need to fetch the full block data
            return if (block.difficulty == null || block.transactions == null) {
                Mono.just(block.hash)
                    .flatMap { hash ->
                        call(JsonRpcRequest("eth_getBlockByHash", listOf(hash.toHex(), false)))
                            .flatMap { resp ->
                                if (resp.isNull()) {
                                    Mono.error(SilentException("Received null for block $hash"))
                                } else {
                                    Mono.just(resp)
                                }
                            }
                            .flatMap(JsonRpcResponse::requireResult)
                            .map { BlockContainer.fromEthereumJson(it) }
                            .subscribeOn(Schedulers.boundedElastic())
                            .timeout(Defaults.timeoutInternal, Mono.empty())
                    }.repeatWhenEmpty { n ->
                        Repeat.times<Any>(5)
                            .exponentialBackoff(Duration.ofMillis(50), Duration.ofMillis(500))
                            .apply(n)
                    }
                    .timeout(Defaults.timeout, Mono.empty())
                    .onErrorResume { Mono.empty() }
                    .doOnNext {
                        blocks.tryEmitNext(it)
                    }
                    .then()
            } else {
                Mono.fromCallable {
                    blocks.tryEmitNext(BlockContainer.from(block))
                }.then()
            }
        }

        fun call(originalRequest: JsonRpcRequest): Mono<JsonRpcResponse> {
            return Mono.fromCallable {
                val startTime = System.nanoTime()
                // use an internal id sequence, to avoid id conflicts with user calls
                val internalId = sendIdSeq.getAndIncrement()
                val originalId = originalRequest.id
                Tuples.of(originalRequest.copy(id = internalId), originalId, startTime)
            }.flatMap { request ->
                waitForResponse(request.t1, request.t2, request.t3)
            }
        }

        fun sendRpc(request: JsonRpcRequest) {
            // submit to upstream in a separate thread, to free current thread (needs for subscription, etc)
            sendExecutor.execute {
                val result = rpcSend.tryEmitNext(request)
                if (result.isFailure) {
                    log.warn("Failed to send RPC request: $result")
                }
            }
        }

        fun waitForResponse(request: JsonRpcRequest, originalId: Int, startTime: Long): Mono<JsonRpcResponse> {
            val expectedId = request.id.toLong()
            return Mono.just(request)
                .flatMap {
                    Flux.from(rpcReceive.asFlux())
                        .doOnSubscribe { sendRpc(request) }
                        .filter { resp -> resp.id.asNumber() == expectedId }
                        .take(Defaults.timeout)
                        .take(1)
                        .singleOrEmpty()
                        .doOnNext {
                            rpcMetrics?.timer?.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS)
                        }
                        .doOnError {
                            rpcMetrics?.errors?.increment()
                        }
                        .map { it.copyWithId(JsonRpcResponse.Id.from(originalId)) }
                        .defaultIfEmpty(
                            JsonRpcResponse(
                                null,
                                JsonRpcError(
                                    RpcResponseError.CODE_INTERNAL_ERROR,
                                    "Response not received from WebSocket"
                                ),
                                JsonRpcResponse.Id.from(originalId)
                            )
                        )
                }
        }

        fun getBlocksFlux(): Flux<BlockContainer> {
            return this.blocks.asFlux()
        }

        override fun close() {
            log.info("Closing connection to WebSocket $uri")
            keepConnection = false
            connection?.dispose()
            connection = null
        }
    }
}
