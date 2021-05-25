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
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import io.infinitape.etherjar.rpc.ws.SubscriptionJson
import io.netty.buffer.ByteBufInputStream
import io.netty.handler.codec.http.HttpHeaderNames
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.netty.http.client.HttpClient
import reactor.netty.http.client.WebsocketClientSpec
import reactor.retry.Repeat
import java.io.InputStream
import java.net.URI
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class EthereumWsFactory(
        private val uri: URI,
        private val origin: URI
) {

    var basicAuth: AuthConfig.ClientBasicAuth? = null

    fun create(upstream: EthereumUpstream): EthereumWs {
        return EthereumWs(uri, origin, upstream, basicAuth)
    }

    class EthereumWs(
            private val uri: URI,
            private val origin: URI,
            private val upstream: EthereumUpstream,
            private val basicAuth: AuthConfig.ClientBasicAuth?
    ) : AutoCloseable {

        companion object {
            private val log = LoggerFactory.getLogger(EthereumWs::class.java)

            private const val START_REQUEST = "{\"jsonrpc\":\"2.0\", \"method\":\"eth_subscribe\", \"id\":\"blocks\", \"params\":[\"newHeads\"]}"
        }

        private val topic = Sinks
                .many()
                .unicast()
                .onBackpressureBuffer<BlockContainer>()
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
            connection = null

            val subscriptionId = AtomicReference<String>("NOTSET")

            val objectMapper = Global.objectMapper
            connection = HttpClient.create()
                    .doOnError(
                            { _, t ->
                                log.warn("Failed to connect to $uri. Error: ${t.message}")
                                // going to try to reconnect later
                                tryReconnectLater()
                            },
                            { _, _ ->

                            }
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
                        if (uri.scheme == "wss") {
                            it.secure()
                        } else {
                            it
                        }
                    }
                    .websocket(
                            WebsocketClientSpec.builder()
                                    .handlePing(true)
                                    .compress(false)
                                    .build()
                    )

                    .uri(uri)
                    .handle { inbound, outbound ->
                        val consumer = inbound.aggregateFrames()
                                .aggregateFrames(8 * 65_536)
                                .receiveFrames()
                                .flatMap {
                                    val msg: SubscriptionJson = objectMapper.readerFor(SubscriptionJson::class.java)
                                            .readValue(ByteBufInputStream(it.content()) as InputStream)
                                    when {
                                        msg.error != null -> {
                                            Mono.error(IllegalStateException("Received error from WS upstream"))
                                        }
                                        msg.subscription == subscriptionId.get() -> {
                                            onNewBlock(msg.blockResult)
                                            Mono.empty<Int>()
                                        }
                                        msg.subscription == null -> {
                                            // received ID for subscription
                                            subscriptionId.set(msg.result.asText())
                                            log.debug("Connected to $uri")
                                            Mono.empty<Int>()
                                        }
                                        else -> {
                                            Mono.error(IllegalStateException("Unknown message received: ${msg.subscription}"))
                                        }
                                    }
                                }
                                .onErrorResume { t ->
                                    log.warn("Connection dropped to $uri. Error: ${t.message}")
                                    // going to try to reconnect later
                                    tryReconnectLater()
                                    // completes current outbound flow
                                    Mono.empty()
                                }


                        outbound.sendString(Mono.just(START_REQUEST).doOnError {
                            println("!!!!!!!")
                        })
                                .then(consumer.then())
                    }.subscribe()
        }

        fun onNewBlock(block: BlockJson<TransactionRefJson>) {
            // WS returns incomplete blocks, i.e. without some fields, so need to fetch full block data
            if (block.difficulty == null || block.transactions == null) {
                Mono.just(block.hash)
                        .flatMap { hash ->
                            upstream.getApi()
                                    .read(JsonRpcRequest("eth_getBlockByHash", listOf(hash.toHex(), false)))
                                    .flatMap { resp ->
                                        if (resp.isNull()) {
                                            Mono.error(SilentException("Received null for block $hash"))
                                        } else {
                                            Mono.just(resp)
                                        }
                                    }
                                    .flatMap(JsonRpcResponse::requireResult)
                                    .map { BlockContainer.fromEthereumJson(it) }
                        }.repeatWhenEmpty { n ->
                            Repeat.times<Any>(5)
                                    .exponentialBackoff(Duration.ofMillis(50), Duration.ofMillis(500))
                                    .apply(n)
                        }
                        .timeout(Defaults.timeout, Mono.empty())
                        .onErrorResume { Mono.empty() }
                        .subscribe {
                            topic.tryEmitNext(it)
                        }

            } else {
                topic.tryEmitNext(BlockContainer.from(block))
            }
        }

        fun getFlux(): Flux<BlockContainer> {
            return this.topic.asFlux()
        }

        override fun close() {
            keepConnection = false
            connection?.dispose()
            connection = null
        }
    }



}