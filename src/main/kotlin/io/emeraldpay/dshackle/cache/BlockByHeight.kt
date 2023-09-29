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

/**
 * Connects two caches to read through them. First is cache height->hash, second is hash->block.
 */
open class BlockByHeight(
    private val heights: Reader<Long, BlockId>,
    private val blocks: Reader<BlockId, BlockContainer>,
) : Reader<Long, BlockContainer> {

    companion object {
        private val log = LoggerFactory.getLogger(BlockByHeight::class.java)
    }

    override fun read(key: Long): Mono<BlockContainer> {
        return heights.read(key)
            .flatMap { blocks.read(it) }
    }
}
