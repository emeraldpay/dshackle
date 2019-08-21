package io.emeraldpay.dshackle.cache

import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

class BlocksMemCache(
        val maxSize: Int = 64
) {

    private val mapping = ConcurrentHashMap<BlockHash, BlockJson<TransactionId>>()
    private val queue = ConcurrentLinkedQueue<BlockHash>()

    fun get(hash: BlockHash): Mono<BlockJson<TransactionId>> {
        return Mono.justOrEmpty(mapping[hash])
    }

    fun add(block: BlockJson<TransactionId>) {
        mapping.put(block.hash, block)
        queue.add(block.hash)

        while (queue.size > maxSize) {
            val old = queue.remove()
            mapping.remove(old)
        }
    }
}