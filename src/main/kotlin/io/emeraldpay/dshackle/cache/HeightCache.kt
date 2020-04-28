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

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap

/**
 * Memory cache for blocks heights, keeps mapping height->hash.
 */
open class HeightCache(
        val maxSize: Int = 256
) : Reader<Long, BlockId> {

    companion object {
        private val log = LoggerFactory.getLogger(HeightCache::class.java)
    }

    private val heights = ConcurrentHashMap<Long, BlockId>()

    override fun read(key: Long): Mono<BlockId> {
        return Mono.justOrEmpty(heights[key])
    }

    open fun add(block: BlockContainer): BlockId? {
        val existing = heights[block.height]
        heights[block.height] = block.hash

        // evict old numbers if full
        var dropHeight = block.height - maxSize
        while (heights.size > maxSize && dropHeight < block.height) {
            heights.remove(dropHeight)
            dropHeight++
        }

        return existing
    }
}