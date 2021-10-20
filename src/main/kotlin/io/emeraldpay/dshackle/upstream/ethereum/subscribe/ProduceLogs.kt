/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import com.google.common.cache.CacheBuilder
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.json.LogMessage
import io.emeraldpay.etherjar.hex.HexData
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty
import java.util.concurrent.TimeUnit

class ProduceLogs(
        private val receipts: Reader<TxId, ByteArray>
) {

    companion object {
        private val log = LoggerFactory.getLogger(ProduceLogs::class.java)
    }

    constructor(upstream: EthereumMultistream) : this(upstream.getReader().receipts())

    private val objectMapper = Global.objectMapper

    // need to keep history of recent messages in case they get removed. cannot rely on
    // any other cache or upstream because if when it gets removed it's unavailable in any other source
    private val oldMessages = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.HOURS)
            .build<LogReference, List<LogMessage>>()

    fun produce(block: Flux<ConnectBlockUpdates.Update>): Flux<LogMessage> {
        return block.flatMap { update ->
            if (update.type == ConnectBlockUpdates.UpdateType.DROP) {
                produceRemoved(update)
            } else {
                produceAdded(update)
            }
        }
    }

    fun produceRemoved(update: ConnectBlockUpdates.Update): Flux<LogMessage> {
        val old = oldMessages.getIfPresent(LogReference(update.blockHash, update.transactionId))
        if (old == null) {
            log.warn("No old message to produce removal messages for tx ${update.transactionId} at block ${update.blockHash}")
            return Flux.empty()
        }
        return Flux.fromIterable(old)
                .map { it.copy(removed = true) }
    }

    fun produceAdded(update: ConnectBlockUpdates.Update): Flux<LogMessage> {
        return receipts.read(update.transactionId)
                .switchIfEmpty {
                    log.warn("Cannot find receipt for tx ${update.transactionId}")
                    Mono.empty()
                }
                .map { objectMapper.readValue(it, TransactionReceiptJson::class.java) }
                .flatMapMany { receipt ->
                    try {
                        val messages = receipt.logs
                                .map { txlog ->
                                    LogMessage(
                                            txlog.address,
                                            txlog.blockHash,
                                            txlog.blockNumber,
                                            txlog.data ?: HexData.empty(),
                                            txlog.logIndex,
                                            txlog.topics,
                                            txlog.transactionHash,
                                            txlog.transactionIndex,
                                            false
                                    )
                                }
                        oldMessages.put(LogReference(update.blockHash, update.transactionId), messages)
                        Flux.fromIterable(messages)
                    } catch (t: Throwable) {
                        log.warn("Invalid Receipt ${update.transactionId}. ${t.javaClass}: ${t.message}")
                        Flux.empty<LogMessage>()
                    }
                }
    }

    private data class LogReference(
            val block: BlockId,
            val tx: TxId
    )
}