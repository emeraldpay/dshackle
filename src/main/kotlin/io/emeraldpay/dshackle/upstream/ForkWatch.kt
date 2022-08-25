/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
import io.emeraldpay.grpc.Chain
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.EnumMap
import java.util.function.Function

/**
 * A watcher that follows a ForkChoice for the blockchain to inform it about new blocks and notify back with the current status.
 * Based on the logic provided by the ForkChoice instance
 */
open class ForkWatch(
    private val forkChoice: ForkChoice,
    private val chain: Chain,
) : Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(ForkWatch::class.java)
    }

    private val metrics = EnumMap<ForkChoice.Status, Counter>(ForkChoice.Status::class.java).also { metrics ->
        ForkChoice.Status.values().forEach { status ->
            metrics[status] = Counter.builder("forkwatch.status.bychain")
                .tag("type", forkChoice.getName())
                .tag("status", status.name)
                .tag("chain", chain.chainCode)
                .register(Metrics.globalRegistry)
        }
    }

    /**
     * Keep it as a separate Sink because it needs to start only when Lifecycle#start is executed, but
     * still needs to return a reference to a Flux with changes anytime when #register is called
     */
    private val changes = Sinks.many()
        .multicast()
        .directBestEffort<Boolean>()

    private var upstream: Upstream? = null
    private var subscription: Disposable? = null

    /**
     * Listed on blockchain head and give a feedback on each block about a potential FORK status.
     * Flux produces TRUE only when the upstream is in a forked state. And FALSE when it goes back to the recognized chain.
     */
    open fun register(upstream: Upstream): Flux<Boolean> {
        this.upstream = upstream
        return changes.asFlux()
    }

    private fun isForkedMapper(source: Upstream): Function<BlockContainer, Boolean> {
        return Function { block -> isForked(block, source) }
    }

    /**
     * Check if the block renders the upstream as forked
     */
    private fun isForked(block: BlockContainer, source: Upstream): Boolean {
        val status = try {
            forkChoice.submit(block, source)
        } catch (t: Throwable) {
            log.error("Failed to check for a fork", t)
            ForkChoice.Status.REJECTED
        }
        metrics[status]?.increment()
        return !status.isOk
    }

    /**
     * A dummy Fork Watch which never forks. Mostly for testing purposes
     */
    class Never : ForkWatch(NeverForkChoice(), Chain.UNSPECIFIED) {
        override fun register(upstream: Upstream): Flux<Boolean> {
            return Flux.just(false)
        }
    }

    override fun start() {
        subscription?.dispose()
        subscription = Flux.interval(Duration.ofMillis(100))
            .flatMap {
                val upstream = this.upstream
                if (upstream == null) { Mono.empty() } else { Mono.just(upstream) }
            }.next().flatMapMany { upstream ->
                val head = upstream.getHead()
                head.getFlux()
                    // the following is just to debug if anything goes wrong
                    .doOnSubscribe { log.debug("Start fork watch ${forkChoice.javaClass.name} for upstream ${upstream?.getId()}") }
                    .doFinally { log.warn("Stopping fork watch for upstream ${upstream?.getId()} after receiving ${it.name}") }
                    .doOnError { t -> log.error("ForkWatch processing failed", t) }
                    .map(isForkedMapper(upstream))
            }
            .distinctUntilChanged()
            .subscribe(changes::tryEmitNext)
    }

    override fun stop() {
        subscription?.dispose()
        subscription = null
    }

    override fun isRunning(): Boolean {
        return subscription != null
    }
}
