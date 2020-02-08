package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Memory cache for transactions
 */
open class TxMemCache(
        // usually there is 100-150 tx per block on Ethereum, we keep data for about 32 blocks by default
        private val maxSize: Int = 125 * 32
): Reader<TransactionId, TransactionJson> {

    companion object {
        private val log = LoggerFactory.getLogger(TxMemCache::class.java)
    }

    private val mapping = ConcurrentHashMap<TransactionId, TransactionJson>()
    private val queue = ConcurrentLinkedQueue<TransactionId>()

    override fun read(key: TransactionId): Mono<TransactionJson> {
        return Mono.justOrEmpty(mapping[key])
    }

    open fun evict(block: BlockJson<TransactionRefJson>) {
        block.transactions.forEach {
            mapping.remove(it.hash)
        }
    }

    open fun evict(block: BlockHash) {
        val ids = mapping.filter { it.value.blockHash == block }
        ids.forEach {
            mapping.remove(it.key)
        }
    }

    open fun add(tx: TransactionJson) {
        //do not cache fresh transactions
        if (tx.blockHash == null || tx.blockNumber == null) {
            return
        }
        mapping.put(tx.hash, tx)
        queue.add(tx.hash)

        while (queue.size > maxSize) {
            val old = queue.remove()
            mapping.remove(old)
        }
    }

}