/**
 * Copyright (c) 2020 EmeraldPay, Inc
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

import com.google.protobuf.ByteString
import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.proto.CachesProto
import io.emeraldpay.dshackle.proto.CachesProto.ValueContainer
import io.emeraldpay.dshackle.reader.Reader
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.math.min

abstract class OnBlockRedisCache<T>(
    private val redis: RedisReactiveCommands<String, ByteArray>,
    private val chain: Chain,
    private val valueType: ValueContainer.ValueType
) : Reader<BlockId, T> {

    companion object {
        private val log = LoggerFactory.getLogger(OnBlockRedisCache::class.java)

        private const val MAX_CACHE_TIME_MINUTES = 60L

        // doesn't make sense to cached in redis short living objects
        private const val MIN_CACHE_TIME_SECONDS = 3
    }

    private val prefix: String = when (valueType) {
        ValueContainer.ValueType.BLOCK -> "block"
        else -> throw IllegalStateException("No prefix for value type $valueType")
    }

    fun toProto(block: BlockContainer, value: T): ValueContainer {
        return ValueContainer.newBuilder()
            .setType(valueType)
            .setValue(ByteString.copyFrom(serializeValue(value)))
            .setBlockMeta(buildMeta(block))
            .build()
    }

    open fun buildMeta(block: BlockContainer): CachesProto.BlockMeta.Builder {
        return CachesProto.BlockMeta.newBuilder()
            .setHash(ByteString.copyFrom(block.hash.value))
            .setHeight(block.height)
            .setDifficulty(ByteString.copyFrom(block.difficulty.toByteArray()))
            .setTimestamp(block.timestamp.toEpochMilli())
    }

    abstract fun serializeValue(value: T): ByteArray

    fun fromProto(msg: ByteArray): T {
        val value = ValueContainer.parseFrom(msg)
        if (value.type != valueType) {
            val error = "Expected $valueType value, received ${value.type}"
            log.warn(error)
            throw IllegalArgumentException(error)
        }
        return deserializeValue(value)
    }

    abstract fun deserializeValue(value: ValueContainer): T

    /**
     * Key in Redis
     */
    fun key(hash: BlockId): String {
        return "$prefix:${chain.id}:${hash.toHex()}"
    }

    /**
     * Add to cache.
     * Note that it returns Mono<Void> which must be subscribed to actually save
     */
    open fun add(container: BlockContainer, value: T): Mono<Void> {
        return Mono.just(container)
            .flatMap { block ->
                val ttl = cachingTime(block.timestamp)
                if (ttl > MIN_CACHE_TIME_SECONDS) {
                    val key = key(block.hash)
                    val proto = toProto(block, value)
                    redis.setex(key, ttl, proto.toByteArray())
                } else {
                    Mono.empty()
                }
            }
            .doOnError {
                log.warn("Failed to save Block to Redis: ${it.message}")
            }
            // if failed to cache, just continue without it
            .onErrorResume {
                Mono.empty()
            }
            .then()
    }

    /**
     * Calculate time to cache the value
     */
    fun cachingTime(blockTime: Instant): Long {
        // default caching time is age of the block, i.e. block create hour ago
        // keep for hour, but block created 10 seconds ago cache only for 10 seconds, because it
        // still can be replaced in the blockchain
        val age = Instant.now().epochSecond - blockTime.epochSecond
        return min(age, TimeUnit.MINUTES.toSeconds(MAX_CACHE_TIME_MINUTES))
    }

    fun evict(id: BlockId): Mono<Void> {
        return Mono.just(id)
            .flatMap {
                redis.del(key(it))
            }
            .then()
    }

    override fun read(key: BlockId): Mono<T> {
        return redis.get(key(key))
            .map { data ->
                fromProto(data)
            }.onErrorResume {
                Mono.empty()
            }
    }
}
