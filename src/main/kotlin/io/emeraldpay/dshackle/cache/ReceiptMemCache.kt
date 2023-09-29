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
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

/**
 * Keeps receipts for recent blocks in memory
 */
open class ReceiptMemCache(
    // how many blocks to keeps in memory
    val blocks: Int = 6,
) : Reader<TxId, ByteArray> {

    companion object {
        private val log = LoggerFactory.getLogger(ReceiptMemCache::class.java)
    }

    private val mapping = Caffeine.newBuilder()
        .maximumSize(blocks * 200L)
        .build<TxId, ByteArray>()

    open fun evict(block: BlockContainer) {
        block.transactions.forEach {
            mapping.invalidate(it)
        }
    }

    override fun read(key: TxId): Mono<ByteArray> {
        return mapping.getIfPresent(key)?.let { Mono.just(it) } ?: Mono.empty()
    }

    open fun add(receipt: DefaultContainer<TransactionReceiptJson>): Mono<Void> {
        if (receipt.txId != null && receipt.json != null) {
            mapping.put(receipt.txId, receipt.json)
        }
        return Mono.empty()
    }

    open fun acceptsRecentBlocks(heightDelta: Long): Boolean {
        return blocks <= heightDelta && heightDelta >= 0
    }
}
