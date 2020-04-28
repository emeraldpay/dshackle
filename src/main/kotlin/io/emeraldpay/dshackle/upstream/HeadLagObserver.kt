/**
 * Copyright (c) 2020 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.data.BlockContainer
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.util.function.Tuple2
import reactor.util.function.Tuples

/**
 * Observer group of upstreams and defined a distance in blocks (lag) between a leader (best height/difficulty) and
 * other upstreams.
 */
abstract class HeadLagObserver<A : UpstreamApi>(
        private val master: Head,
        private val followers: Collection<Upstream<A>>
) : Lifecycle {

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

    fun subscription(): Flux<Unit> {
        return master.getFlux()
                .flatMap(this::probeFollowers)
                .map { item ->
                    item.t2.setLag(item.t1)
                }
    }

    fun probeFollowers(top: BlockContainer): Flux<Tuple2<Long, Upstream<A>>> {
        return Flux.fromIterable(followers)
                .parallel(followers.size)
                .flatMap { mapLagging(top, it, getCurrentBlocks(it)) }
                .sequential()
                .onErrorContinue { t, _ -> log.warn("Failed to update lagging distance", t) }
    }

    abstract fun getCurrentBlocks(up: Upstream<A>): Flux<BlockContainer>

    fun mapLagging(top: BlockContainer, up: Upstream<A>, blocks: Flux<BlockContainer>): Flux<Tuple2<Long, Upstream<A>>> {
        return blocks
                .map { extractDistance(top, it) }
                .takeUntil { lag -> lag <= 0L }
                .map { Tuples.of(it, up) }
                .doOnError { t ->
                    log.warn("Failed to find distance for $up", t)
                }
    }

    abstract fun extractDistance(top: BlockContainer, curr: BlockContainer): Long

    fun forkDistance(top: BlockContainer, curr: BlockContainer): Long {
        //TODO look for common ancestor? though it may be a corruption
        return 6
    }

}