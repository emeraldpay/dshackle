package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.reader.CompoundReader
import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor

open class Caches(
        private val memBlocksByHash: BlocksMemCache,
        private val blocksByHeight: HeightCache,
        private val memTxsByHash: TxMemCache,
        private val redisBlocksByHash: BlocksRedisCache?,
        private val redisTxsByHash: TxRedisCache?
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

    private val blocksByHash: Reader<BlockHash, BlockJson<TransactionRefJson>>
    private val txsByHash: Reader<TransactionId, TransactionJson>

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
    }

    /**
     * Cache data that was just requested
     */
    fun cacheRequested(data: Any) {
        if (data is TransactionJson) {
            cache(Tag.REQUESTED, data)
        } else if (data is BlockJson<*>) {
            cache(Tag.REQUESTED, data as BlockJson<TransactionRefJson>)
        }
    }

    fun cache(tag: Tag, tx: TransactionJson) {
        //do not cache transactions that are not in a block yet
        if (tx.blockHash == null) {
            return
        }
        memTxsByHash.add(tx)
        memBlocksByHash.get(tx.blockHash)?.let { block ->
            redisTxsByHash?.add(tx, block)
        }
    }

    fun cache(tag: Tag, block: BlockJson<TransactionRefJson>) {
        val job = ArrayList<Mono<Void>>()
        if (tag == Tag.LATEST) {
            //for LATEST data cache in memory, it will be short living so better to avoid Redis
            memBlocksByHash.add(block)
            val replaced = blocksByHeight.add(block)
            //evict cached transactions if an existing block was updated
            replaced?.let { replacedBlockHash ->
                var evicted = false
                redisBlocksByHash?.evict(replacedBlockHash)
                memBlocksByHash.get(replacedBlockHash)?.let { block ->
                    memTxsByHash.evict(block)
                    redisTxsByHash?.evict(block)
                    evicted = true
                }
                if (!evicted) {
                    memTxsByHash.evict(replacedBlockHash)
                }
            }
        } else if (tag == Tag.REQUESTED) {
            //shouldn't cache block json with transactions, separate txes and blocks with refs
            val blockOnly = block.withoutTransactionDetails()
            memBlocksByHash.add(blockOnly)
            redisBlocksByHash?.add(blockOnly)?.let(job::add)

            // now cache only transactions
            val transactions = block.transactions.filterIsInstance<TransactionJson>()
            if (transactions.isNotEmpty()) {
                transactions.forEach { cache(Tag.REQUESTED, it) }
                if (redisTxsByHash != null) {
                    job.add(Flux.fromIterable(transactions).flatMap { redisTxsByHash.add(it, block) }.then())
                }
            }
        }
        Flux.fromIterable(job).flatMap { it }.subscribe() //TODO move out to a caller
    }

    fun getBlocksByHash(): Reader<BlockHash, BlockJson<TransactionRefJson>> {
        return blocksByHash
    }

    fun getBlockHashByHeight(): Reader<Long, BlockHash> {
        return blocksByHeight
    }

    fun getBlocksByHeight(): Reader<Long, BlockJson<TransactionRefJson>> {
        return BlockByHeight(blocksByHeight, blocksByHash)
    }

    fun getTxByHash(): Reader<TransactionId, TransactionJson> {
        return txsByHash
    }

    fun getFullBlocks(): Reader<BlockHash, BlockJson<TransactionJson>> {
        return BlocksWithTxCache(blocksByHash, txsByHash)
    }

    fun getFullBlocksByHeight(): Reader<Long, BlockJson<TransactionJson>> {
        return BlockByHeight(blocksByHeight, BlocksWithTxCache(blocksByHash, txsByHash))
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
            return Caches(blocksByHash!!, blocksByHeight!!, txsByHash!!, redisBlocksByHash, redisTxsByHash)
        }
    }
}