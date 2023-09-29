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
package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.TimeUnit

/**
 * Cache for height by hash.
 * Different from blocks cache because for height we don't really care about eviction and replaced blocks (also a fall-back block
 * reader would use full block's cache to find out height).
 */
class HeightByHashRedisCache(
    private val redis: RedisReactiveCommands<String, ByteArray>,
    private val chain: Chain,
) : Reader<BlockId, Long>, HeightByHashCache {

    companion object {
        private val log = LoggerFactory.getLogger(HeightByHashRedisCache::class.java)

        private const val MAX_CACHE_TIME_MINUTES = 60L * 4
    }

    @Suppress("UNCHECKED_CAST")
    override fun read(key: BlockId): Mono<Long> {
        return redis.get(key(key))
            .flatMap { data ->
                Mono.justOrEmpty(fromBytes(data)) as Mono<Long>
            }.onErrorResume {
                log.warn("Failed to read Block Height. ${it.javaClass}:${it.message}")
                Mono.empty()
            }
    }

    override fun add(block: BlockContainer): Mono<Void> {
        return Mono.just(block)
            .flatMap { blockData ->
                // even if block replaced, the mapping hash-long is still valid, so can be cached for long time
                // even for fresh blocks
                val ttl = TimeUnit.MINUTES.toSeconds(MAX_CACHE_TIME_MINUTES)

                val key = key(blockData.hash)
                val value = asBytes(blockData.height)
                redis.setex(key, ttl, value)
            }
            .doOnError {
                log.warn("Failed to save Block Height. ${it.javaClass}:${it.message}")
            }
            // if failed to cache, just continue without it
            .onErrorResume {
                Mono.empty()
            }
            .then()
    }

    fun asBytes(value: Long): ByteArray {
        val result = ByteArray(8)
        val bb = ByteBuffer.allocate(8)
            .order(ByteOrder.BIG_ENDIAN)
        bb.asLongBuffer()
            .put(value)
        bb.get(result)
        return result
    }

    fun fromBytes(value: ByteArray): Long? {
        if (value.size != 8) {
            return null
        }
        return ByteBuffer.wrap(value)
            .order(ByteOrder.BIG_ENDIAN)
            .asLongBuffer()
            .get()
    }

    /**
     * Key in Redis
     */
    fun key(hash: BlockId): String {
        return "height:${chain.id}:${hash.toHex()}"
    }
}
