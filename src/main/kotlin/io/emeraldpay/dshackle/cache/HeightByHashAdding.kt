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

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

/**
 * Height By Hash reader, that adds to Redis cache missing values.
 *
 * Tries to read value from current memory cache, then from redis if it's available, and if both empty read from blockchain.
 * With the latter case it adds read value back to the redis cache (again, if available).
 *
 */
class HeightByHashAdding(
    private val mem: Reader<BlockId, Long>,
    private val redis: HeightByHashCache?,
    private val upstreamReader: Reader<BlockId, BlockContainer>
) : Reader<BlockId, Long> {

    companion object {
        private val log = LoggerFactory.getLogger(HeightByHashAdding::class.java)
    }

    constructor(caches: Caches, upstreamReader: Reader<BlockId, BlockContainer>) :
        this(caches.getLastHeightByHash(), caches.getRedisHeightByHash(), upstreamReader)

    private val delegate: Reader<BlockId, Long>

    init {
        if (redis != null) {
            delegate = object : Reader<BlockId, Long> {
                override fun read(key: BlockId): Mono<Long> {
                    return mem.read(key)
                        .switchIfEmpty(
                            Mono.just(key)
                                .flatMap { redis.read(it) }
                        )
                        .switchIfEmpty(
                            Mono.just(key)
                                .flatMap { upstreamReader.read(it) }
                                .flatMap { redis.add(it).then(Mono.just(it.height)) }
                        )
                }
            }
        } else {
            delegate = object : Reader<BlockId, Long> {
                override fun read(key: BlockId): Mono<Long> {
                    return mem.read(key)
                        .switchIfEmpty(
                            Mono.just(key)
                                .flatMap { upstreamReader.read(it) }
                                .map { it.height }
                        )
                }
            }
        }
    }

    override fun read(key: BlockId): Mono<Long> {
        return delegate.read(key)
    }
}
