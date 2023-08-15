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

import com.github.benmanes.caffeine.cache.Caffeine
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class HeightByHashMemCache(
    maxSize: Int = 256
) : Reader<BlockId, Long> {

    companion object {
        private val log = LoggerFactory.getLogger(HeightByHashMemCache::class.java)
    }

    private val heights = Caffeine.newBuilder()
        .maximumSize(maxSize.toLong())
        .build<BlockId, Long>()

    override fun read(key: BlockId): Mono<Long> {
        return Mono.justOrEmpty(heights.getIfPresent(key))
    }

    fun get(key: BlockId): Long? {
        return heights.getIfPresent(key)
    }

    fun add(block: BlockContainer) {
        heights.put(block.hash, block.height)
    }
}
