/**
 * Copyright (c) 2020 EmeraldPay, Inc
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

import com.github.benmanes.caffeine.cache.Caffeine
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import reactor.core.publisher.Mono

open class BlocksMemCache(
        maxSize: Int = 64
) : Reader<BlockId, BlockContainer> {

    private val mapping = Caffeine.newBuilder()
            .maximumSize(maxSize.toLong())
            .build<BlockId, BlockContainer>()

    override fun read(key: BlockId): Mono<BlockContainer> {
        return Mono.justOrEmpty(get(key))
    }

    open fun get(key: BlockId): BlockContainer? {
        return mapping.getIfPresent(key)
    }

    open fun add(block: BlockContainer) {
        mapping.put(block.hash, block)
    }

    open fun purge() {
        mapping.cleanUp()
    }
}