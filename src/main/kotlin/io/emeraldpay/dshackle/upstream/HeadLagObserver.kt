/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.toFlux
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.io.Closeable
import java.time.Duration

class HeadLagObserver (
        private val master: EthereumHead,
        private val followers: Collection<Upstream>
): Lifecycle {

    private val log = LoggerFactory.getLogger(HeadLagObserver::class.java)

    private var current: Disposable? = null

    override fun start() {
        current = subscription().subscribe { }
    }

    override fun isRunning(): Boolean {
        return current != null
    }

    override fun stop() {
        current?.dispose()
        current = null
    }

    private fun subscription(): Flux<Unit> {
        return master.getFlux()
                .flatMap(this::probeFollowers)
                .map { item ->
                    item.t2.setLag(item.t1)
                }
    }

    fun probeFollowers(top: BlockJson<TransactionId>): Flux<Tuple2<Long, Upstream>> {
        return followers.toFlux()
                .parallel(followers.size)
                .flatMap { mapLagging(top, it, getCurrentBlocks(it)) }
                .sequential()
                .onErrorContinue { t, _ -> log.warn("Failed to update lagging distance", t) }
    }

    fun getCurrentBlocks(up: Upstream): Flux<BlockJson<TransactionId>> {
        val head = up.getHead()
        return head.getFlux().take(Duration.ofSeconds(1))
    }

    fun mapLagging(top: BlockJson<TransactionId>, up: Upstream, blocks: Flux<BlockJson<TransactionId>>): Flux<Tuple2<Long, Upstream>> {
        return blocks
                .map { extractDistance(top, it) }
                .takeUntil{ lag -> lag <= 0L }
                .map { Tuples.of(it, up) }
                .doOnError { t ->
                    log.warn("Failed to find distance for $up", t)
                }
    }

    fun extractDistance(top: BlockJson<TransactionId>, curr: BlockJson<TransactionId>): Long {
        return  when {
            curr.number  > top.number -> if (curr.totalDifficulty >= top.totalDifficulty) 0 else forkDistance(top, curr)
            curr.number == top.number -> if (curr.totalDifficulty == top.totalDifficulty) 0 else forkDistance(top, curr)
            else -> top.number - curr.number
        }
    }

    fun forkDistance(top: BlockJson<TransactionId>, curr: BlockJson<TransactionId>): Long {
        //TODO look for common ancestor? though it may be a corruption
        return 6
    }

}