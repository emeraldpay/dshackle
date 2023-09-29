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
import reactor.core.scheduler.Scheduler
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.time.Duration

/**
 * Observer group of upstreams and defined a distance in blocks (lag) between a leader (best height/difficulty) and
 * other upstreams.
 */
typealias Extractor = (top: BlockContainer, curr: BlockContainer) -> DistanceExtractor.ChainDistance
abstract class HeadLagObserver(
    private val master: Head,
    private val followers: Collection<Upstream>,
    private val distanceExtractor: Extractor,
    private val lagObserverScheduler: Scheduler,
    private val throttling: Duration = Duration.ofSeconds(5),
) : Lifecycle {

    private val log = LoggerFactory.getLogger(HeadLagObserver::class.java)

    private var current: Disposable? = null

    override fun start() {
        current?.dispose()
        current = subscription().subscribeOn(lagObserverScheduler).subscribe { }
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
            .sample(throttling)
            .flatMap(this::probeFollowers)
            .map { item ->
                log.debug("Set lag ${item.t1} to upstream ${item.t2.getId()}")
                item.t2.setLag(item.t1)
            }
    }

    fun probeFollowers(top: BlockContainer): Flux<Tuple2<Long, Upstream>> {
        log.debug("Compute lag for ${followers.map { it.getId() }}")

        return Flux.fromIterable(followers)
            .parallel(followers.size)
            .flatMap { up -> mapLagging(top, up, getCurrentBlocks(up)).subscribeOn(lagObserverScheduler) }
            .sequential()
            .onErrorContinue { t, _ -> log.warn("Failed to update lagging distance", t) }
    }

    open fun getCurrentBlocks(up: Upstream): Flux<BlockContainer> {
        val head = up.getHead()
        return head.getFlux().take(Duration.ofSeconds(1))
    }

    fun mapLagging(top: BlockContainer, up: Upstream, blocks: Flux<BlockContainer>): Flux<Tuple2<Long, Upstream>> {
        return blocks
            .map { extractDistance(top, it) }
            .takeUntil { lag -> lag <= 0L }
            .map { Tuples.of(it, up) }
            .doOnError { t ->
                log.warn("Failed to find distance for $up", t)
            }
            .doOnNext {
                log.debug("Lag for ${it.t2.getId()} is ${it.t1}")
            }
    }

    open fun extractDistance(top: BlockContainer, curr: BlockContainer): Long {
        return when (val distance = distanceExtractor(top, curr)) {
            is DistanceExtractor.ChainDistance.Distance -> distance.dist
            is DistanceExtractor.ChainDistance.Fork -> forkDistance(top, curr)
        }
    }

    abstract fun forkDistance(top: BlockContainer, curr: BlockContainer): Long
}
