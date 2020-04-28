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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.grpc.Chain
import io.emeraldpay.dshackle.proto.CachesProto
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.util.function.Tuples
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.math.min

/**
 * Cache transactions in Redis, up to 24 hours.
 */
class TxRedisCache(
        private val redis: RedisReactiveCommands<String, ByteArray>,
        private val chain: Chain
) : Reader<TxId, TxContainer> {

    companion object {
        private val log = LoggerFactory.getLogger(TxRedisCache::class.java)

        // max caching time is 24 hours
        private const val MAX_CACHE_TIME_HOURS = 24L
    }

    override fun read(key: TxId): Mono<TxContainer> {
        return redis.get(key(key))
                .map { data ->
                    fromProto(data)
                }.onErrorResume {
                    Mono.empty()
                }
    }

    fun toProto(value: TxContainer): ByteArray {
        val meta = CachesProto.TxMeta.newBuilder()
                .setHash(ByteString.copyFrom(value.hash.value))
                .setHeight(value.height)

        value.blockId?.value?.let {
            meta.setBlockHash(ByteString.copyFrom(it))
        }

        return CachesProto.ValueContainer.newBuilder()
                .setType(CachesProto.ValueContainer.ValueType.TX)
                .setValue(ByteString.copyFrom(value.json!!))
                .setTxMeta(meta)
                .build()
                .toByteArray()
    }

    fun fromProto(msg: ByteArray): TxContainer {
        val value = CachesProto.ValueContainer.parseFrom(msg)
        if (value.type != CachesProto.ValueContainer.ValueType.TX) {
            throw IllegalArgumentException("Expect TX value, receive ${value.type}")
        }
        if (!value.hasTxMeta()) {
            throw IllegalArgumentException("Container doesn't have Tx Meta")
        }
        val meta = value.txMeta
        return TxContainer(
                meta.height,
                TxId(meta.hash.toByteArray()),
                BlockId(meta.blockHash.toByteArray()),
                value.value.toByteArray()
        )
    }

    fun evict(block: BlockContainer): Mono<Void> {
        return Mono.just(block)
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

    fun add(tx: TxContainer, block: BlockContainer): Mono<Void> {
        if (tx.blockId == null || block.hash == null || tx.blockId != block.hash || block.timestamp == null) {
            return Mono.empty()
        }
        return Mono.just(Tuples.of(tx, block))
                .flatMap {
                    val key = key(it.t1.hash)
                    val value = toProto(it.t1)
                    //default caching time is age of the block, i.e. block create hour ago
                    //keep for hour, but block create 10 seconds ago cache for 10 seconds, as it
                    //still can be replaced in the blockchain
                    val age = Instant.now().epochSecond - it.t2.timestamp!!.epochSecond
                    val ttl = min(age, TimeUnit.HOURS.toSeconds(MAX_CACHE_TIME_HOURS))
                    //store
                    redis.setex(key, ttl, value)
                }
                .doOnError {
                    log.warn("Failed to save TX to Redis: ${it.message}", it)
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
    fun key(hash: TxId): String {
        return "tx:${chain.id}:${hash.toHex()}"
    }
}