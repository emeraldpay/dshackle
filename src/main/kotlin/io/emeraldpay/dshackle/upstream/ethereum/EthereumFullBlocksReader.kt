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

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.nio.ByteBuffer
import java.util.function.BiFunction

/**
 * Reads blocks with full transactions details. Based on data contained in readers for blocks
 * and transactions, i.e. two separate readers that must be provided.
 *
 * If source block, with just transaction hashes is not available, it returns empty
 * If any of the expected block transactions is not available it returns empty
 */
class EthereumFullBlocksReader(
        private val blocks: Reader<BlockId, BlockContainer>,
        private val txes: Reader<TxId, TxContainer>
) : Reader<BlockId, BlockContainer> {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumFullBlocksReader::class.java)
    }

    private val accumulate: BiFunction<ByteBuffer, ByteArray, ByteBuffer> = BiFunction { buf, x ->
        if (buf.remaining() < x.size) {
            val resize = ByteBuffer.allocate(buf.capacity() + buf.capacity() / 4 + x.size)
            resize.put(buf.flip()).put(x)
        } else {
            buf.put(x)
        }
    }

    override fun read(key: BlockId): Mono<BlockContainer> {
        return blocks.read(key).flatMap { block ->
            if (block.transactions.isEmpty()) {
                // in fact it's not necessary to create a copy, made just for code clarity but it may be a performance loss
                val fullBlock = BlockContainer(
                        block.height, block.hash, block.difficulty, block.timestamp,
                        true,
                        block.json,
                        block.parsed,
                        block.transactions
                )
                return@flatMap Mono.just(fullBlock)
            }

            val blockSplit = splitByTransactions(block.json!!)

            val transactions = Flux.fromIterable(block.transactions)
                    .flatMap { txes.read(it) }
                    .collectList()

            return@flatMap transactions.flatMap { transactionsData ->
                // make sure that all transaction are loaded, otherwise just return empty because cannot make full block data
                if (transactionsData.size != block.transactions.size) {
                    log.warn("No data to fill the block")
                    Mono.empty()
                } else {
                    joinWithTransactions(blockSplit.t1, blockSplit.t2, Flux.fromIterable(transactionsData).map { it.json!! })
                            .reduce(ByteBuffer.allocate(block.json.size * 4), accumulate)
                            .map { it.flip().array() }
                            .map { json ->
                                BlockContainer(block.height, block.hash, block.difficulty, block.timestamp,
                                        true,
                                        json,
                                        null,
                                        block.transactions
                                )
                            }
                }
            }
        }
    }

    fun splitByTransactions(json: ByteArray): Tuple2<ByteArray, ByteArray> {
        //TODO find a lib that implements Knuth-Morris-Pratt Pattern Matching Algorithm for byte arrays
        // and reimplement without making a string copy from bytes

        val s = String(json)
        val fieldStart = s.indexOf("\"transactions\"")
        val arrayStart = s.indexOf("[", fieldStart)
        val arrayEnd = s.indexOf("]", arrayStart)

        val head = s.substring(0, arrayStart + 1)
        val tail = s.substring(arrayEnd, s.length)

        return Tuples.of(head.toByteArray(), tail.toByteArray())
    }

    fun joinWithTransactions(head: ByteArray, tail: ByteArray, transactions: Flux<ByteArray>): Flux<ByteArray> {
        val separator = Flux.range(0, Integer.MAX_VALUE)
                .map { it != 0 }

        val transactionsWithSeparator = transactions.zipWith(separator)
                .flatMap {
                    val tx = Flux.just(it.t1)
                    if (it.t2) {
                        Flux.concat(Flux.just(",".toByteArray()), tx)
                    } else {
                        tx
                    }
                }

        return Flux.concat(
                Flux.just(head),
                transactionsWithSeparator,
                Flux.just(tail)
        )
    }

}