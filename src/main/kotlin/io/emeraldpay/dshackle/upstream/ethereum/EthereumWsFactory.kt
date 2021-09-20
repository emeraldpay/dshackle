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
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.ResponseWSParser
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.HttpHeaderNames
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
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
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


class EthereumWsFactory(
        private val uri: URI,
        private val origin: URI
) {

    var basicAuth: AuthConfig.ClientBasicAuth? = null

    fun create(): EthereumWs {
        return EthereumWs(uri, origin, basicAuth)
    }

    class EthereumWs(
            private val uri: URI,
            private val origin: URI,
            private val basicAuth: AuthConfig.ClientBasicAuth?
    ) : AutoCloseable {

        companion object {
            private val log = LoggerFactory.getLogger(EthereumWs::class.java)

            private const val IDS_START = 100
            private const val START_REQUEST = "{\"jsonrpc\":\"2.0\", \"method\":\"eth_subscribe\", \"id\":\"blocks\", \"params\":[\"newHeads\"]}"
        }

        private val parser = ResponseWSParser()

        private val blocks = Sinks
                .many()
                .multicast()
                .directBestEffort<BlockContainer>()
        private val rpcSend = Sinks
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

        fun connect() {
            if (keepConnection) {
                connectInternal()
            }
        }

        private fun tryReconnectLater() {
            Global.control.schedule(
                    { connectInternal() },
                    Defaults.retryConnection.seconds, TimeUnit.SECONDS)
        }

        private fun connectInternal() {
            log.info("Connecting to WebSocket: $uri")
            connection?.dispose()
            connection = HttpClient.create()
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
                                    .build()
                    )
                    .uri(uri)
                    .handle { inbound, outbound ->
                        handle(inbound, outbound)
                    }
                    .doOnError {
                        log.error("Failed to setup WS connection", it)
                    }
                    .subscribe()
        }

        fun handle(inbound: WebsocketInbound, outbound: WebsocketOutbound): Publisher<Void> {
            val consumer = inbound.aggregateFrames()
                    // accept up to 1Mb messages
                    .aggregateFrames(16 * 65_536)
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
                        log.warn("Connection dropped to $uri. Error: ${t.message}")
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
                //it's a response to the newHeads subscription, just ignore it
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
                // use an internal id sequence, to avoid id conflicts with user calls
                val internalId = sendIdSeq.getAndIncrement()
                val originalId = originalRequest.id
                Tuples.of(originalRequest.copy(id = internalId), originalId)
            }.flatMap { request ->
                waitForResponse(request.t1, request.t2)
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

        fun waitForResponse(request: JsonRpcRequest, originalId: Int): Mono<JsonRpcResponse> {
            val expectedId = request.id.toLong()
            return Mono.just(request)
                    .flatMap {
                        Flux.from(rpcReceive.asFlux())
                                .doOnSubscribe { sendRpc(request) }
                                .filter { resp -> resp.id.asNumber() == expectedId }
                                .take(1)
                                .singleOrEmpty()
                                .map { it.copyWithId(JsonRpcResponse.Id.from(originalId)) }
                    }
        }

        fun getBlocksFlux(): Flux<BlockContainer> {
            return this.blocks.asFlux()
        }

        override fun close() {
            keepConnection = false
            connection?.dispose()
            connection = null
        }
    }


}