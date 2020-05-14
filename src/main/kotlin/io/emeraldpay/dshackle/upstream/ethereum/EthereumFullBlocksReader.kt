/**
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.ethereum

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
 * Reads blocks with full transactions details. Based on data contained in readers for blocks
 * and transactions, i.e. two separate readers that must be provided.
 *
 * If source block, with just transaction hashes is not available, it returns empty
 * If any of the expected block transactions is not available it returns empty
 */
class EthereumFullBlocksReader(
        private val objectMapper: ObjectMapper,
        private val blocks: Reader<BlockId, BlockContainer>,
        private val txes: Reader<TxId, TxContainer>
) : Reader<BlockId, BlockContainer> {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumFullBlocksReader::class.java)
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