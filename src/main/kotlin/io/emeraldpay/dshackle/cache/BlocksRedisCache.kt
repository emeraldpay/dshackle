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

/**
 * Cache blocks in Redis database
 */
class BlocksRedisCache(
        redis: RedisReactiveCommands<String, ByteArray>,
        chain: Chain
) : Reader<BlockId, BlockContainer>,
        OnBlockRedisCache<BlockContainer>(redis, chain, CachesProto.ValueContainer.ValueType.BLOCK) {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksRedisCache::class.java)
    }

    override fun buildMeta(block: BlockContainer): CachesProto.BlockMeta.Builder {
        val meta = super.buildMeta(block)
        block.transactions.forEach {
            meta.addTxHashes(ByteString.copyFrom(it.value))
        }
        return meta
    }

    override fun serializeValue(value: BlockContainer): ByteArray {
        return value.json!!
    }

    override fun deserializeValue(value: CachesProto.ValueContainer): BlockContainer {
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

    fun add(block: BlockContainer): Mono<Void> {
        if (block.timestamp == null || block.hash == null) {
            return Mono.empty()
        }
        if (block.full) {
            return Mono.error(IllegalArgumentException("Full Block is not supposed to be cached"))
        }
        return super.add(block, block)
    }

}