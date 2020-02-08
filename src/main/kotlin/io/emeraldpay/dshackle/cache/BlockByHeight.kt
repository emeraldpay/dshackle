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
open class BlockByHeight(
        private val heights: Reader<Long, BlockHash>,
        private val blocks: Reader<BlockHash, BlockJson<TransactionRefJson>>
): Reader<Long, BlockJson<TransactionRefJson>> {

    companion object {
        private val log = LoggerFactory.getLogger(BlockByHeight::class.java)
    }

    override fun read(key: Long): Mono<BlockJson<TransactionRefJson>> {
        return heights.read(key)
                .flatMap { blocks.read(it) }
    }

}