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

import com.github.benmanes.caffeine.cache.Caffeine
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import reactor.core.publisher.Mono

/**
 * Memory cache for blocks heights, keeps mapping height->hash.
 */
open class HeightCache(
    maxSize: Int = 512,
) : Reader<Long, BlockId> {

    private val heights = Caffeine.newBuilder()
        .maximumSize(maxSize.toLong())
        .build<Long, BlockId>()

    override fun read(key: Long): Mono<BlockId> {
        return Mono.justOrEmpty(heights.getIfPresent(key))
    }

    open fun add(block: BlockContainer): BlockId? {
        val previousId = heights.getIfPresent(block.height)
        heights.put(block.height, block.hash)
        return previousId
    }

    fun purge() {
        heights.cleanUp()
    }
}
