package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

/**
 * Connects two caches to read through them. First is cache height->hash, second is hash->block.
 */
open class BlockByHeight(
        private val heights: Reader<Long, BlockId>,
        private val blocks: Reader<BlockId, BlockContainer>
) : Reader<Long, BlockContainer> {

    companion object {
        private val log = LoggerFactory.getLogger(BlockByHeight::class.java)
    }

    override fun read(key: Long): Mono<BlockContainer> {
        return heights.read(key)
                .flatMap { blocks.read(it) }
    }

}