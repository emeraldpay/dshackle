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

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.*
import io.emeraldpay.dshackle.reader.CompoundReader
import io.emeraldpay.dshackle.reader.EmptyReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.EthereumFullBlocksReader
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionReceiptJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

open class Caches(
        private val memBlocksByHash: BlocksMemCache,
        private val blocksByHeight: HeightCache,
        private val memTxsByHash: TxMemCache,
        private val redisBlocksByHash: BlocksRedisCache?,
        private val redisTxsByHash: TxRedisCache?,
        private val redisReceipts: ReceiptRedisCache?
) {

    companion object {
        private val log = LoggerFactory.getLogger(Caches::class.java)

        @JvmStatic
        fun newBuilder(): Builder {
            return Builder()
        }

        @JvmStatic
        fun default(): Caches {
            return newBuilder().build()
        }
    }

    private val blocksByHash: Reader<BlockId, BlockContainer>
    private val txsByHash: Reader<TxId, TxContainer>
    private val receiptByHash: Reader<TxId, ByteArray>

    init {
        blocksByHash = if (redisBlocksByHash == null) {
            memBlocksByHash
        } else {
            CompoundReader(memBlocksByHash, redisBlocksByHash)
        }
        txsByHash = if (redisTxsByHash == null) {
            memTxsByHash
        } else {
            CompoundReader(memTxsByHash, redisTxsByHash)
        }
        receiptByHash = redisReceipts ?: EmptyReader()
    }

    fun setHead(head: Head) {
        redisTxsByHash?.head = head
        redisReceipts?.head = head
    }

    /**
     * Cache data that was just requested
     */
    fun cacheRequested(data: Any) {
        if (data is TxContainer) {
            cache(Tag.REQUESTED, data)
        } else if (data is BlockContainer) {
            cache(Tag.REQUESTED, data)
        }
    }

    open fun cacheReceipt(tag: Tag, data: DefaultContainer<TransactionReceiptJson>) {
        //TODO move subscription to the caller
        redisReceipts?.add(data)?.subscribe()
    }

    fun cache(tag: Tag, tx: TxContainer) {
        //do not cache transactions that are not in a block yet
        if (tx.blockId == null) {
            return
        }
        memTxsByHash.add(tx)
        //TODO move subscription to the caller
        getBlocksByHash().read(tx.blockId).flatMap { block ->
            redisTxsByHash?.add(tx, block) ?: Mono.empty()
        }.subscribe()
    }

    fun cache(tag: Tag, block: BlockContainer) {
        val job = ArrayList<Mono<Void>>()
        if (tag == Tag.LATEST) {
            //for LATEST data cache it in memory, it may be short living so better to avoid Redis
            memoizeBlock(block)
        } else if (tag == Tag.REQUESTED) {
            var blockOnlyContainer: BlockContainer? = null
            var jsonValue: BlockJson<*>? = null
            if (block.full) {
                jsonValue = Global.objectMapper.readValue<BlockJson<*>>(block.json, BlockJson::class.java)
                //shouldn't cache block json with transactions, separate txes and blocks with refs
                val blockOnly = jsonValue.withoutTransactionDetails()
                blockOnlyContainer = BlockContainer.from(blockOnly)
            } else {
                blockOnlyContainer = block
            }
            memoizeBlock(blockOnlyContainer)
            memBlocksByHash.add(blockOnlyContainer)
            redisBlocksByHash?.add(blockOnlyContainer)?.let(job::add)

            // now cache only transactions
            jsonValue?.let { jsonValue ->
                val plainTransactions = jsonValue.transactions.filterIsInstance<TransactionJson>()
                if (plainTransactions.isNotEmpty()) {
                    val transactions = plainTransactions.map { tx ->
                        TxContainer.from(tx)
                    }
                    if (redisTxsByHash != null) {
                        job.add(Flux.fromIterable(transactions)
                                .doOnNext { memTxsByHash.add(it) }
                                .flatMap { redisTxsByHash.add(it, block) }
                                .then())
                    }
                }
            }
        }
        Flux.fromIterable(job).flatMap { it }.subscribe() //TODO move out to a caller
    }

    /**
     * Cache the block only in memory
     */
    fun memoizeBlock(block: BlockContainer) {
        memBlocksByHash.add(block)
        val replaced = blocksByHeight.add(block)
        //evict cached transactions if an existing block was updated
        replaced?.let { evict(it) }
    }

    fun evict(blockId: BlockId) {
        var evicted = false
        redisBlocksByHash?.evict(blockId)
        memBlocksByHash.get(blockId)?.let { block ->
            memTxsByHash.evict(block)
            redisTxsByHash?.evict(block)
            evicted = true
        }
        if (!evicted) {
            memTxsByHash.evict(blockId)
        }
    }

    fun getBlocksByHash(): Reader<BlockId, BlockContainer> {
        return blocksByHash
    }

    fun getBlockHashByHeight(): Reader<Long, BlockId> {
        return blocksByHeight
    }

    fun getBlocksByHeight(): Reader<Long, BlockContainer> {
        return BlockByHeight(blocksByHeight, blocksByHash)
    }

    fun getTxByHash(): Reader<TxId, TxContainer> {
        return txsByHash
    }

    fun getFullBlocks(): Reader<BlockId, BlockContainer> {
        return EthereumFullBlocksReader(blocksByHash, txsByHash)
    }

    fun getFullBlocksByHeight(): Reader<Long, BlockContainer> {
        return BlockByHeight(blocksByHeight, EthereumFullBlocksReader(blocksByHash, txsByHash))
    }

    fun getReceipts(): Reader<TxId, ByteArray> {
        return receiptByHash
    }

    enum class Tag {
        /**
         * Latest data produced by blockchain
         */
        LATEST,

        /**
         * Data requested by client
         */
        REQUESTED
    }

    class Builder() {
        private var blocksByHash: BlocksMemCache? = null
        private var blocksByHeight: HeightCache? = null
        private var txsByHash: TxMemCache? = null
        private var redisBlocksByHash: BlocksRedisCache? = null
        private var redisTxsByHash: TxRedisCache? = null
        private var redisReceiptCache: ReceiptRedisCache? = null

        fun setBlockByHash(cache: BlocksMemCache): Builder {
            blocksByHash = cache
            return this
        }

        fun setBlockByHash(cache: BlocksRedisCache): Builder {
            redisBlocksByHash = cache
            return this
        }

        fun setBlockByHeight(cache: HeightCache): Builder {
            blocksByHeight = cache
            return this
        }

        fun setTxByHash(cache: TxMemCache): Builder {
            txsByHash = cache
            return this
        }

        fun setTxByHash(cache: TxRedisCache): Builder {
            redisTxsByHash = cache
            return this
        }

        fun setReceipts(cache: ReceiptRedisCache): Builder {
            redisReceiptCache = cache
            return this
        }

        fun build(): Caches {
            if (blocksByHash == null) {
                blocksByHash = BlocksMemCache()
            }
            if (blocksByHeight == null) {
                blocksByHeight = HeightCache()
            }
            if (txsByHash == null) {
                txsByHash = TxMemCache()
            }
            return Caches(blocksByHash!!, blocksByHeight!!, txsByHash!!, redisBlocksByHash, redisTxsByHash, redisReceiptCache)
        }
    }
}