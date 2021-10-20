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
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.proto.CachesProto
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.grpc.Chain
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.math.min

abstract class OnTxRedisCache<T>(
    private val redis: RedisReactiveCommands<String, ByteArray>,
    private val chain: Chain,
    private val valueType: CachesProto.ValueContainer.ValueType
) : Reader<TxId, T> {

    companion object {
        private val log = LoggerFactory.getLogger(OnTxRedisCache::class.java)

        // max caching time is 24 hours
        const val MAX_CACHE_TIME_HOURS = 24L
        const val MIN_CACHE_TIME_SECONDS = 30L
        const val BLOCK_TIME_SECONDS = 10L
    }

    private val prefix: String = when (valueType) {
        CachesProto.ValueContainer.ValueType.TX -> "tx"
        CachesProto.ValueContainer.ValueType.TX_RECEIPT -> "tx-receipt"
        else -> throw IllegalStateException("No prefix for value type $valueType")
    }

    var head: Head? = null

    /**
     * Key in Redis
     */
    fun key(hash: TxId): String {
        return "$prefix:${chain.id}:${hash.toHex()}"
    }

    fun evict(container: BlockContainer): Mono<Void> {
        return Mono.just(container)
            .map { block ->
                block.transactions.map {
                    key(it)
                }.toTypedArray()
            }.flatMap { keys ->
                redis.del(*keys)
            }.then()
    }

    fun evict(id: TxId): Mono<Void> {
        return Mono.just(id)
            .flatMap {
                redis.del(key(it))
            }
            .then()
    }

    fun toProto(id: TxId, value: T): ByteArray {
        val meta = buildMeta(id, value)

        return CachesProto.ValueContainer.newBuilder()
            .setType(valueType)
            .setValue(ByteString.copyFrom(serializeValue(value)))
            .setTxMeta(meta)
            .build()
            .toByteArray()
    }

    open fun buildMeta(id: TxId, value: T): CachesProto.TxMeta.Builder {
        return CachesProto.TxMeta.newBuilder()
            .setHash(ByteString.copyFrom(id.value))
    }

    abstract fun serializeValue(value: T): ByteArray

    fun fromProto(msg: ByteArray): T {
        val value = CachesProto.ValueContainer.parseFrom(msg)
        if (value.type != valueType) {
            val error = "Expected $valueType value, received ${value.type}"
            log.warn(error)
            throw IllegalArgumentException(error)
        }
        return deserializeValue(value)
    }

    abstract fun deserializeValue(value: CachesProto.ValueContainer): T

    override fun read(key: TxId): Mono<T> {
        return redis.get(key(key))
            .map { data ->
                fromProto(data)
            }.onErrorResume {
                Mono.empty()
            }
    }

    open fun add(id: TxId, value: T, block: BlockContainer?, blockHeight: Long?): Mono<Void> {
        return Mono.just(id)
            .flatMap {
                val key = key(it)
                val encodedValue = toProto(it, value)
                val ttl = if (block?.timestamp != null) {
                    cachingTime(block.timestamp)
                } else {
                    cachingTime(blockHeight)
                }
                // store
                redis.setex(key, ttl, encodedValue)
            }
            .doOnError {
                log.warn("Failed to save TX to Redis: ${it.message}", it)
            }
            // if failed to cache, just continue without it
            .onErrorResume {
                Mono.empty()
            }
            .then()
    }

    /**
     * Calculate time to cache the value, based on block time
     */
    fun cachingTime(blockTime: Instant): Long {
        // default caching time is age of the block, i.e. block create hour ago
        // keep for hour, but block create 10 seconds ago cache for 10 seconds, as it
        // still can be replaced in the blockchain
        val age = Instant.now().epochSecond - blockTime.epochSecond
        return min(age, TimeUnit.HOURS.toSeconds(MAX_CACHE_TIME_HOURS))
    }

    /**
     * Calculate time to cache the value, based on block height
     */
    fun cachingTime(blockHeight: Long?): Long {
        if (blockHeight == null) {
            return MIN_CACHE_TIME_SECONDS
        }
        val headHeight = head?.getCurrentHeight() ?: return MIN_CACHE_TIME_SECONDS
        val confirmations = headHeight - blockHeight
        if (confirmations <= 0) {
            return MIN_CACHE_TIME_SECONDS
        }
        return min(confirmations * BLOCK_TIME_SECONDS, TimeUnit.HOURS.toSeconds(MAX_CACHE_TIME_HOURS))
    }
}
