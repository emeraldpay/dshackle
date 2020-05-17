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
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.proto.CachesProto
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.grpc.Chain
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.math.min

/**
 * Cache blocks in Redis database
 */
class BlocksRedisCache(
        private val redis: RedisReactiveCommands<String, ByteArray>,
        private val chain: Chain
) : Reader<BlockId, BlockContainer> {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksRedisCache::class.java)
        private const val MAX_CACHE_TIME_MINUTES = 60L

        // doesn't make sense to cached in redis short living objects
        private const val MIN_CACHE_TIME_SECONDS = 10
    }

    override fun read(key: BlockId): Mono<BlockContainer> {
        return redis.get(key(key))
                .map { data ->
                    fromProto(data)
                }.onErrorResume {
                    Mono.empty()
                }
    }

    fun toProto(value: BlockContainer): ByteArray {
        if (value.full) {
            throw IllegalArgumentException("Full Block is not supposed to be cached")
        }
        val meta = CachesProto.BlockMeta.newBuilder()
                .setHash(ByteString.copyFrom(value.hash.value))
                .setHeight(value.height)
                .setDifficulty(ByteString.copyFrom(value.difficulty.toByteArray()))
                .setTimestamp(value.timestamp.toEpochMilli())

        value.transactions.forEach {
            meta.addTxHashes(ByteString.copyFrom(it.value))
        }

        return CachesProto.ValueContainer.newBuilder()
                .setType(CachesProto.ValueContainer.ValueType.BLOCK)
                .setValue(ByteString.copyFrom(value.json!!))
                .setBlockMeta(meta)
                .build()
                .toByteArray()
    }

    fun fromProto(msg: ByteArray): BlockContainer {
        val value = CachesProto.ValueContainer.parseFrom(msg)
        if (value.type != CachesProto.ValueContainer.ValueType.BLOCK) {
            throw IllegalArgumentException("Expect BLOCK value, receive ${value.type}")
        }
        if (!value.hasBlockMeta()) {
            throw IllegalArgumentException("Container doesn't have Block Meta")
        }
        val meta = value.blockMeta
        return BlockContainer(
                meta.height,
                BlockId(meta.hash.toByteArray()),
                BigInteger(meta.difficulty.toByteArray()),
                Instant.ofEpochMilli(meta.timestamp),
                false,
                value.value.toByteArray(),
                null,
                meta.txHashesList.map {
                    TxId(it.toByteArray())
                }
        )
    }

    fun evict(id: BlockId): Mono<Void> {
        return Mono.just(id)
                .flatMap {
                    redis.del(key(it))
                }
                .then()
    }

    /**
     * Add to cache.
     * Note that it returns Mono<Void> which must be subscribed to actually save
     */
    fun add(block: BlockContainer): Mono<Void> {
        if (block.timestamp == null || block.hash == null) {
            return Mono.empty()
        }
        return Mono.just(block)
                .flatMap { block ->
                    //default caching time is age of the block, i.e. block create hour ago
                    //keep for hour, but block create 10 seconds ago cache for 10 seconds, as it
                    //still can be replaced in the blockchain
                    val age = Instant.now().epochSecond - block.timestamp!!.epochSecond
                    val ttl = min(age, TimeUnit.MINUTES.toSeconds(MAX_CACHE_TIME_MINUTES))
                    if (ttl > MIN_CACHE_TIME_SECONDS) {
                        val key = key(block.hash)
                        val value = toProto(block)
                        redis.setex(key, ttl, value)
                    } else {
                        Mono.empty()
                    }
                }
                .doOnError {
                    log.warn("Failed to save Block to Redis: ${it.message}")
                }
                //if failed to cache, just continue without it
                .onErrorResume {
                    Mono.empty()
                }
                .then()
    }

    /**
     * Key in Redis
     */
    fun key(hash: BlockId): String {
        return "block:${chain.id}:${hash.toHex()}"
    }
}