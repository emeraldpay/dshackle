package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap

/**
 * Memory cache for blocks heights, keeps mapping height->hash.
 */
class HeightCache(
        val maxSize: Int = 256
): Reader<Long, BlockHash> {

    companion object {
        private val log = LoggerFactory.getLogger(HeightCache::class.java)
    }

    private val heights = ConcurrentHashMap<Long, BlockHash>()

    override fun read(key: Long): Mono<BlockHash> {
        return Mono.justOrEmpty(heights[key])
    }

    fun add(block: BlockJson<TransactionRefJson>) {
        heights[block.number] = block.hash

        // evict old numbers if full
        var dropHeight = block.number - maxSize
        while (heights.size > maxSize && dropHeight < block.number) {
            heights.remove(dropHeight)
            dropHeight++
        }
    }
}