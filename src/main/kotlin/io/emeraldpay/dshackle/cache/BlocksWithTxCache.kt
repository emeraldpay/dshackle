package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import org.slf4j.LoggerFactory
import org.springframework.beans.BeanUtils
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Reads blocks with full transactions details. Based on data contained in cashes for blocks
 * and transactions, i.e. two separate caches that must be provided.
 *
 * If source block, with just transaction hashes is not available, it returns empty
 * If any of the expected block transactions is not available it returns empty
 */
class BlocksWithTxCache(
        private val blocks: BlocksMemCache,
        private val txes: TxMemCache
): Reader<BlockHash, BlockJson<TransactionJson>> {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksWithTxCache::class.java)
    }

    override fun read(key: BlockHash): Mono<BlockJson<TransactionJson>> {
        return blocks.read(key).flatMap { block ->
            if (block.transactions == null || block.transactions.isEmpty()) {
                // in fact it's not necessary to create a copy, made just for code clarity but may be performance loss
                val fullBlock = BlockJson<TransactionJson>()
                BeanUtils.copyProperties(block, fullBlock)
                Mono.just(fullBlock)
            } else {
                Flux.fromIterable(block.transactions)
                        .map { it.hash }
                        .flatMap { txes.read(it) }
                        .collectList()
                        .flatMap { list ->
                            if (block.transactions.size != list.size) {
                                Mono.empty<BlockJson<TransactionJson>>()
                            } else {
                                val fullBlock = BlockJson<TransactionJson>()
                                BeanUtils.copyProperties(block, fullBlock)
                                fullBlock.transactions = list
                                Mono.just(fullBlock)
                            }
                        }
            }
        }
    }

}