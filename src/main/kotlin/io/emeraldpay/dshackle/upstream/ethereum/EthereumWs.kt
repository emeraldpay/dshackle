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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.EmptyReader
import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
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
        private val api: EthereumApi,
        private val objectMapper: ObjectMapper
): CachesEnabled {

    private val log = LoggerFactory.getLogger(EthereumWs::class.java)
    private val topic = TopicProcessor
            .builder<BlockContainer>()
            .name("new-blocks")
            .build()
    var basicAuth: AuthConfig.ClientBasicAuth? = null

    private var blockCache: Reader<BlockId, BlockContainer> = EmptyReader()

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
            Mono.just(block.hash).flatMap { hash ->
                val hash = BlockId.from(hash)
                // first check in cache, if empty then check api
                blockCache.read(hash)
                        .switchIfEmpty(request(hash))
            }.repeatWhenEmpty { n ->
                Repeat.times<Any>(10)
                        .exponentialBackoff(Duration.ofMillis(50), Duration.ofMillis(250))
                        .apply(n)
            }
                    .timeout(Defaults.timeout, Mono.empty())
                    .subscribe(topic::onNext)

        } else {
            topic.onNext(BlockContainer.from(block, objectMapper))
        }
    }

    fun request(hash: BlockId): Mono<BlockContainer> {
        return api
                .executeAndConvert(Commands.eth().getBlock(io.infinitape.etherjar.domain.BlockHash(hash.value)))
                .map { BlockContainer.from(it, objectMapper) }
    }

    fun getFlux(): Flux<BlockContainer> {
        return Flux.from(this.topic)
                .onBackpressureLatest()
    }

    override fun setCaches(caches: Caches) {
        blockCache = caches.getBlocksByHash()
    }
}