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
open class TxRedisCache(
        private val redis: RedisReactiveCommands<String, ByteArray>,
        private val chain: Chain
) : Reader<TxId, TxContainer>,
        OnTxRedisCache<TxContainer>(redis, chain, CachesProto.ValueContainer.ValueType.TX) {

    companion object {
        private val log = LoggerFactory.getLogger(TxRedisCache::class.java)
    }

    override fun buildMeta(id: TxId, value: TxContainer): CachesProto.TxMeta.Builder {
        val meta = super.buildMeta(id, value)
        value.height?.let {
            meta.setHeight(it)
        }

        value.blockId?.value?.let {
            meta.setBlockHash(ByteString.copyFrom(it))
        }
        return meta
    }

    override fun serializeValue(value: TxContainer): ByteArray {
        return value.json!!
    }

    override fun deserializeValue(value: CachesProto.ValueContainer): TxContainer {
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

    open fun add(tx: TxContainer, block: BlockContainer): Mono<Void> {
        return super.add(tx.hash, tx, block, tx.height)
    }

}