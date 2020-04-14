/**
 * Copyright (c) 2019 ETCDEV GmbH
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
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

open class BlocksMemCache(
        val maxSize: Int = 64
) : Reader<BlockId, BlockContainer> {

    private val mapping = ConcurrentHashMap<BlockId, BlockContainer>()
    private val queue = ConcurrentLinkedQueue<BlockId>()

    override fun read(key: BlockId): Mono<BlockContainer> {
        return Mono.justOrEmpty(mapping[key])
    }

    open fun get(key: BlockId): BlockContainer? {
        return mapping[key]
    }

    open fun add(block: BlockContainer) {
        mapping.put(block.hash, block)
        queue.add(block.hash)

        while (queue.size > maxSize) {
            val old = queue.remove()
            mapping.remove(old)
        }
    }

}