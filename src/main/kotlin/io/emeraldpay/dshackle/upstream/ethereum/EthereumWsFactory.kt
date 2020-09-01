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
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import io.infinitape.etherjar.rpc.ws.WebsocketClient
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.extra.processor.TopicProcessor
import reactor.retry.Repeat
import java.net.URI
import java.time.Duration

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
    ) {

        companion object {
            private val log = LoggerFactory.getLogger(EthereumWs::class.java)
        }

        private val topic = TopicProcessor
                .builder<BlockContainer>()
                .name("new-blocks")
                .build()

        fun connect() {
            log.info("Connecting to WebSocket: $uri")
            val clientBuilder = WebsocketClient.newBuilder()
                    .connectTo(uri)
                    .origin(origin)
            basicAuth?.let { auth ->
                clientBuilder.basicAuth(auth.username, auth.password)
            }
            val client = clientBuilder.build()
            try {
                client.connect()
                client.onNewBlock(this::onNewBlock)
            } catch (e: Exception) {
                log.error("Failed to connect to websocket at $uri. Error: ${e.message}")
            }
        }

        fun onNewBlock(block: BlockJson<TransactionRefJson>) {
            // WS returns incomplete blocks
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
                        .subscribe(topic::onNext)

            } else {
                topic.onNext(BlockContainer.from(block))
            }
        }

        fun getFlux(): Flux<BlockContainer> {
            return Flux.from(this.topic)
                    .onBackpressureLatest()
        }
    }



}