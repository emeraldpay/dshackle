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

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumLikeMultistream
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.LinkedList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class ConnectBlockUpdates(
    private val upstream: EthereumLikeMultistream
) {

    companion object {
        private val log = LoggerFactory.getLogger(ConnectBlockUpdates::class.java)
        private const val HISTORY_LIMIT = 6 * 3
    }

    /**
     * Need to keep history of few last blocks in case we have got a conflicting blocks on the same height.
     * In this case it produces a list of updates for transactions that are missing from the new version of the block.
     */
    private val history = LinkedList<BlockContainer>()
    private val historyUpdateLock = ReentrantReadWriteLock()
    private val connected: MutableMap<String, Flux<Update>> = ConcurrentHashMap()

    fun connect() = connect(Selector.empty)
    fun connect(matcher: Selector.Matcher): Flux<Update> {
        return connected.computeIfAbsent(matcher.describeInternal()) { key ->
            extract(upstream.getHead(matcher))
                .publishOn(Schedulers.boundedElastic())
                .publish()
                .refCount(1, Duration.ofSeconds(60))
                .doFinally {
                    // forget it on disconnect, so next time it's recreated
                    connected.remove(key)
                }
        }
    }

    fun extract(head: Head): Flux<Update> {
        return head.getFlux()
            .flatMap(this@ConnectBlockUpdates::extract)
    }

    fun extract(block: BlockContainer): Flux<Update> {
        val prev = findPrevious(block)
        remember(block)
        val removed = if (prev != null) {
            whenReplaced(prev)
        } else {
            Flux.empty()
        }
        val added = extractUpdates(block)
        return Flux.concat(removed, added)
    }

    fun findPrevious(block: BlockContainer): BlockContainer? {
        historyUpdateLock.read {
            val existing = history.find { it.height == block.height }
            if (existing != null) {
                historyUpdateLock.write {
                    history.removeIf { it.hash == existing.hash }
                }
            }
            return existing
        }
    }

    fun remember(block: BlockContainer) {
        historyUpdateLock.write {
            history.add(block)
            if (history.size > HISTORY_LIMIT) {
                history.removeFirst()
            }
        }
    }

    /**
     * Produce updates for transactions when a block is replaces with a different one on the same height.
     */
    fun whenReplaced(prev: BlockContainer): Flux<Update> {
        return Flux.fromIterable(prev.transactions).map {
            Update(
                prev.hash,
                prev.height,
                UpdateType.DROP,
                it
            )
        }
    }

    fun extractUpdates(block: BlockContainer): Flux<Update> {
        return Flux.fromIterable(block.transactions)
            .map {
                Update(
                    block.hash,
                    block.height,
                    UpdateType.NEW,
                    it
                )
            }
    }

    data class Update(
        val blockHash: BlockId,
        val blockNumber: Long,
        val type: UpdateType,
        val transactionId: TxId,
    )

    enum class UpdateType {
        NEW,
        DROP
    }
}
