/**
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

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.ws.WebsocketClient
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.retry.Repeat
import java.net.URI
import java.time.Duration

class EthereumWs(
        private val uri: URI,
        private val origin: URI,
        private val api: EthereumApi
) {

    private val log = LoggerFactory.getLogger(EthereumWs::class.java)
    private val topic = TopicProcessor
            .builder<BlockJson<TransactionId>>()
            .name("new-blocks")
            .build()
    var basicAuth: UpstreamsConfig.BasicAuth? = null

    fun connect() {
        log.info("Connecting to WebSocket: $uri")
        val client = WebsocketClient(uri, origin)
        basicAuth?.let { auth ->
            client.setBasicAuth(auth.username, auth.password)
        }
        try {
            client.connect()
        } catch (e: Exception) {
            log.error("Failed to connect to websocket at $uri. Error: ${e.message}")
            return
        }
        client.onNewBlock {
            if (it.totalDifficulty == null || it.transactions == null) {
                Mono.just(it.hash).flatMap { hash ->
                    api.executeAndConvert(Commands.eth().getBlock(hash))
                }.repeatWhenEmpty { n ->
                    Repeat.times<Any>(10)
                            .exponentialBackoff(Duration.ofMillis(50), Duration.ofMillis(250))
                            .apply(n)
                }
                .timeout(Duration.ofSeconds(5), Mono.empty())
                .subscribe(topic::onNext)
            } else {
                topic.onNext(it)
            }
        }
    }

    fun getFlux(): Flux<BlockJson<TransactionId>> {
        return Flux.from(this.topic)
                .onBackpressureLatest()
                .sample(Duration.ofMillis(100))
    }
}