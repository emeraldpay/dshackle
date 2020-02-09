package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

/**
 * Connects two caches to read through them. First is cache height->hash, second is hash->block.
 */
open class BlockByHeight<T: TransactionRefJson>(
        private val heights: Reader<Long, BlockHash>,
        private val blocks: Reader<BlockHash, BlockJson<T>>
): Reader<Long, BlockJson<T>> {

    companion object {
        private val log = LoggerFactory.getLogger(BlockByHeight::class.java)
    }

    override fun read(key: Long): Mono<BlockJson<T>> {
        return heights.read(key)
                .flatMap { blocks.read(it) }
    }

}