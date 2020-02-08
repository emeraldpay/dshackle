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

import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

open class BlocksMemCache(
        val maxSize: Int = 64
): Reader<BlockHash, BlockJson<TransactionRefJson>> {

    private val mapping = ConcurrentHashMap<BlockHash, BlockJson<TransactionRefJson>>()
    private val queue = ConcurrentLinkedQueue<BlockHash>()

    override fun read(key: BlockHash): Mono<BlockJson<TransactionRefJson>> {
        return Mono.justOrEmpty(mapping[key])
    }

    open fun get(key: BlockHash): BlockJson<TransactionRefJson>? {
        return mapping[key]
    }

    open fun add(block: BlockJson<TransactionRefJson>) {
        mapping.put(block.hash, block)
        queue.add(block.hash)

        while (queue.size > maxSize) {
            val old = queue.remove()
            mapping.remove(old)
        }
    }

}