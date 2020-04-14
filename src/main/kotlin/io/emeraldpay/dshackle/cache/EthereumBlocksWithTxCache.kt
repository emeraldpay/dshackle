package io.emeraldpay.dshackle.cache

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
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
class EthereumBlocksWithTxCache(
        private val objectMapper: ObjectMapper,
        private val blocks: Reader<BlockId, BlockContainer>,
        private val txes: Reader<TxId, TxContainer>
) : Reader<BlockId, BlockContainer> {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumBlocksWithTxCache::class.java)
    }

    override fun read(key: BlockId): Mono<BlockContainer> {
        return blocks.read(key).flatMap { block ->
            val block = objectMapper.readValue(block.json, BlockJson::class.java) as BlockJson<TransactionRefJson>
            val fullBlock = if (block.transactions == null || block.transactions.isEmpty()) {
                // in fact it's not necessary to create a copy, made just for code clarity but it may be a performance loss
                val fullBlock = BlockJson<TransactionJson>()
                BeanUtils.copyProperties(block, fullBlock)
                Mono.just(fullBlock)
            } else {
                Flux.fromIterable(block.transactions)
                        .map { TxId.from(it.hash) }
                        .flatMap { txes.read(it) }
                        .collectList()
                        .flatMap { list ->
                            if (block.transactions.size != list.size) {
                                Mono.empty<BlockJson<TransactionJson>>()
                            } else {
                                val fullBlock = BlockJson<TransactionJson>()
                                BeanUtils.copyProperties(block, fullBlock)
                                fullBlock.transactions = list.map {
                                    objectMapper.readValue(it.json, TransactionJson::class.java)
                                }
                                Mono.just(fullBlock)
                            }
                        }
            }
            fullBlock
                    .map { block ->
                        BlockContainer(block.number, BlockId.from(block.hash), block.totalDifficulty, block.timestamp, true,
                                objectMapper.writeValueAsBytes(block),
                                block.transactions.map { tx -> TxId.from(tx) }
                        )
                    }
        }
    }

}