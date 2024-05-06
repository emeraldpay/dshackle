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
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumCachingReader
import io.emeraldpay.dshackle.upstream.ethereum.EthereumDirectReader
import io.emeraldpay.dshackle.upstream.ethereum.EthereumDirectReader.Result
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionLogJson
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.json.LogMessage
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.onErrorResume
import reactor.kotlin.core.publisher.switchIfEmpty
import java.util.concurrent.TimeUnit

class ProduceLogs(
    private val logs: Reader<BlockId, Result<List<TransactionLogJson>>>,
    private val chain: Chain,
) {
    private val MAX_RETRIES = 3
    companion object {
        private val log = LoggerFactory.getLogger(ProduceLogs::class.java)
    }

    constructor(upstream: Multistream) :
        this((upstream.getCachingReader() as EthereumCachingReader).logsByHash(), (upstream as Multistream).chain)

    // need to keep history of recent messages in case they get removed. cannot rely on
    // any other cache or upstream because if when it gets removed it's unavailable in any other source
    private val oldMessages = CacheBuilder.newBuilder()
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .maximumSize(30)
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
        val old = oldMessages.getIfPresent(LogReference(update.blockHash))
        if (old == null) {
            log.warn(
                "No old message to produce removal messages " +
                    "at block ${update.blockHash} for chain ${chain.chainName}",
            )
            return Flux.empty()
        }
        return Flux.fromIterable(old)
            .map { it.copy(removed = true) }
    }

    private fun produceAddedFallback(update: ConnectBlockUpdates.Update, retries: Int): Mono<EthereumDirectReader.Result<List<TransactionLogJson>>> {
        return logs.read(update.blockHash).switchIfEmpty {
            if (retries > MAX_RETRIES) {
                log.warn("Cannot find receipt for block ${update.blockHash} for chain ${chain.chainName} retries so far: $retries")
                Mono.empty()
            } else {
                produceAddedFallback(update, retries + 1)
            }
        }.onErrorResume { t ->
            if (retries > MAX_RETRIES) {
                log.error("Error ${t.message} produced ${update.blockHash} for chain ${chain.chainName} retries so far: $retries")
                Mono.empty()
            } else {
                produceAddedFallback(update, retries + 1)
            }
        }
    }

    fun produceAdded(update: ConnectBlockUpdates.Update): Flux<LogMessage> {
        return produceAddedFallback(update, 0).map {
            it.data
        }.flatMapMany {
            val messages = it.map { log ->
                LogMessage(
                    log.address,
                    log.blockHash,
                    log.blockNumber,
                    log.data ?: HexData.empty(),
                    log.logIndex,
                    log.topics,
                    log.transactionHash,
                    log.transactionIndex,
                    false,
                    update.upstreamId,

                )
            }
            oldMessages.put(LogReference(update.blockHash), messages)
            Flux.fromIterable(messages)
        }
    }

    private data class LogReference(
        val block: BlockId,
    )
}
