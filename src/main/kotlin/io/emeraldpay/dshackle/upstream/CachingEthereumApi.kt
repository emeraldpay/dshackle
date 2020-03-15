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
package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.upstream.ethereum.EmptyEthereumHead
import io.emeraldpay.dshackle.upstream.ethereum.EthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumHead
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.util.function.Function

open class CachingEthereumApi(
        private val objectMapper: ObjectMapper,
        private val caches: Caches,
        private val head: EthereumHead
): EthereumApi(objectMapper) {

    companion object {
        private val log = LoggerFactory.getLogger(CachingEthereumApi::class.java)

        /**
         * Create caching API with empty memory-only cache
         */
        @JvmStatic
        fun empty(): CachingEthereumApi {
            return CachingEthereumApi(ObjectMapper(), Caches.default(), EmptyEthereumHead())
        }
    }

    private val cacheBlocks = caches.getBlocksByHash()
    private val cacheBlocksByHeight = caches.getBlocksByHeight()
    private val cacheTx = caches.getTxByHash()
    private val cacheFullBlocks = caches.getFullBlocks()
    private val cacheFullBlocksByHeight = caches.getFullBlocksByHeight()

    fun readBlockByHash(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        return if (params.size == 2) {
            val includeTransactions = params[1].toString().toBoolean()
            val cache = if (includeTransactions) {
                cacheFullBlocks
            } else {
                cacheBlocks
            }
            Mono.just(params[0])
                    .map { BlockHash.from(it as String) }
                    .flatMap(cache::read)
                    .transform(converter(id))
                    .transform(finalizer())
        }
        else Mono.empty()
    }

    fun readBlockByNumber(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        return if (params.size == 2) {
            val includeTransactions = params[1].toString().toBoolean()
            val cache = if (includeTransactions) {
                cacheFullBlocksByHeight
            } else {
                cacheBlocksByHeight
            }
            Mono.just(params[0])
                    .map { HexQuantity.from(it as String) }
                    .filter { it.value < BigInteger.valueOf(Long.MAX_VALUE) }
                    .map { it.value.toLong() }
                    .flatMap(cache::read)
                    .transform(converter(id))
                    .transform(finalizer())
        }
        else Mono.empty()
    }

    override fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        return when (method) {
            "eth_blockNumber" ->
                head.getFlux().next()
                    .map { HexQuantity.from(it.number).toHex() }
                    .map(toJson(id))
            "eth_getBlockByHash" -> readBlockByHash(id, method, params)
            "eth_getBlockByNumber" -> readBlockByNumber(id, method, params)
            "eth_getTransactionByHash" ->
                if (params.size == 1)
                    Mono.just(params[0])
                            .map { TransactionId.from(it as String) }
                            .flatMap(cacheTx::read)
                            .transform(converter(id))
                            .transform(finalizer())
                else Mono.empty()
            else ->
                Mono.empty()
        }
    }

    /**
     * Convert to JSON RPC response
     */
    fun converter(id: Int): Function<in Mono<*>, out Mono<ByteArray>> {
        return Function { mono ->
            mono.map(toJson(id))
        }
    }

    /**
     * Handle errors and other stuff
     */
    fun finalizer(): Function<Mono<ByteArray>, Mono<ByteArray>> {
        return Function { mono ->
            mono.onErrorResume { t ->
                log.warn("Error during read from cache", t)
                Mono.empty()
            }
        }
    }

    fun toJson(id: Int): Function<Any, ByteArray> {
        return Function { data ->
            val resp = ResponseJson<Any, Int>()
            resp.id = id
            resp.result = data
            objectMapper.writer().writeValueAsBytes(resp)
        }
    }
}