package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory

open class Caches(
        private val blocksByHash: BlocksMemCache,
        private val blocksByHeight: HeightCache,
        private val txsByHash: TxMemCache
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
        txsByHash.add(tx)
    }

    fun cache(tag: Tag, block: BlockJson<TransactionRefJson>) {
        if (tag == Tag.LATEST) {
            blocksByHash.add(block)
            val replaced = blocksByHeight.add(block)
            //evict cached transactions if an existing block was updated
            replaced?.let { replacedBlockHash ->
                var evicted = false
                blocksByHash.get(replacedBlockHash)?.let { block ->
                    txsByHash.evict(block)
                    evicted = true
                }
                if (!evicted) {
                    txsByHash.evict(replacedBlockHash)
                }
            }
        } else if (tag == Tag.REQUESTED) {
            // if block with transactions was requests cache only transactions
            block.transactions.forEach { tx ->
                if (tx is TransactionJson) {
                    cache(Tag.REQUESTED, tx)
                }
            }
        }
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

        fun setBlockByHash(cache: BlocksMemCache): Builder {
            blocksByHash = cache
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
            return Caches(blocksByHash!!, blocksByHeight!!, txsByHash!!)
        }
    }
}