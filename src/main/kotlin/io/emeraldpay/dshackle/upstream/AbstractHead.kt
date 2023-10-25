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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Metrics
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.SignalType
import reactor.core.publisher.Sinks
import reactor.core.publisher.Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER
import reactor.core.publisher.Sinks.EmitResult.OK
import reactor.core.scheduler.Scheduler
import reactor.kotlin.core.publisher.toMono
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

abstract class AbstractHead @JvmOverloads constructor(
    private val forkChoice: ForkChoice,
    private val headScheduler: Scheduler,
    private val blockValidator: BlockValidator = BlockValidator.ALWAYS_VALID,
    private val awaitHeadTimeoutMs: Long = 60_000,
    private val upstreamId: String = "",
) : Head {
    protected val log = LoggerFactory.getLogger(this::class.java)

    companion object {
        private val executor = Executors.newSingleThreadScheduledExecutor()
    }

    private var stream = Sinks.many().multicast().directBestEffort<BlockContainer>()
    private var completed = false
    private val beforeBlockHandlers = ArrayList<Runnable>()
    private var stopping = false
    private var lastHeadUpdated = 0L
    private val lock = ReentrantLock()
    private var future: Future<*>? = null
    private val delayed = AtomicBoolean(false)

    private val metrics = mutableSetOf<Meter>()

    init {
        val className = this.javaClass.simpleName
        Gauge.builder("stuck_head", delayed) {
            if (it.get()) 1.0 else 0.0
        }
            .tag("upstream", upstreamId)
            .tag("class", className)
            .register(Metrics.globalRegistry)
            .also { metrics.add(it) }
        Gauge.builder("current_head", forkChoice) {
            it.getHead()?.height?.toDouble() ?: 0.0
        }
            .tag("upstream", upstreamId)
            .tag("class", className)
            .register(Metrics.globalRegistry)
            .also { metrics.add(it) }
    }

    fun follow(source: Flux<BlockContainer>): Disposable {
        return source
            .filter {
                log.trace("Filtering block $upstreamId block $it")
                forkChoice.filter(it)
            }
            .doFinally {
                // close internal stream if upstream is finished, otherwise it gets stuck,
                // but technically it should never happen during normal work, only when the Head
                // is stopping
                if (it == SignalType.ON_ERROR && !stopping) {
                    log.warn("Received signal $upstreamId $it unexpectedly - restart head")
                    lastHeadUpdated = 0L
                } else {
                    log.warn("Received signal $upstreamId $it, continue emit heads")
                }
            }
            .subscribeOn(headScheduler)
            .subscribe { block ->
                val valid = runCatching {
                    blockValidator.isValid(forkChoice.getHead(), block)
                }.onFailure {
                    log.error("Block $upstreamId ${block.hash} validation failed with '${it.message}'", it)
                }.getOrElse { false }
                if (valid) {
                    notifyBeforeBlock()
                    when (val choiceResult = forkChoice.choose(block)) {
                        is ForkChoice.ChoiceResult.Updated -> {
                            val newHead = choiceResult.nwhead
                            lastHeadUpdated = System.currentTimeMillis()
                            when (val result = stream.tryEmitNext(newHead)) {
                                OK -> log.trace("New block $upstreamId ${newHead.height} ${newHead.hash} @ ${this.javaClass}")
                                FAIL_ZERO_SUBSCRIBER -> log.trace("No subscribers $upstreamId ${this.javaClass}")
                                else -> log.warn("Failed to dispatch block $upstreamId: $result as ${this.javaClass}")
                            }
                        }

                        is ForkChoice.ChoiceResult.Same -> {}
                    }
                } else {
                    log.warn("Invalid block $upstreamId $block}")
                }
            }
    }

    fun notifyBeforeBlock() {
        beforeBlockHandlers.forEach { handler ->
            try {
                handler.run()
            } catch (t: Throwable) {
                log.warn("Before Block handler error", t)
            }
        }
    }

    override fun onBeforeBlock(handler: Runnable) {
        beforeBlockHandlers.add(handler)
    }

    override fun getFlux(): Flux<BlockContainer> {
        return Flux.concat(
            forkChoice.getHead().toMono(),
            stream.asFlux(),
        ).onBackpressureLatest()
    }

    fun getCurrent(): BlockContainer? {
        return forkChoice.getHead()
    }

    override fun getCurrentHeight(): Long? {
        return getCurrent()?.height
    }

    override fun stop() {
        stopping = true
        future?.let {
            it.cancel(true)
        }
        future = null
        metrics.forEach { Metrics.globalRegistry.remove(it) }
    }

    protected open fun onNoHeadUpdates() {
        // NOOP
    }

    override fun onSyncingNode(isSyncing: Boolean) {
        // NOOP
    }

    override fun start() {
        stopping = false
        log.debug("Start ${this.javaClass.simpleName} $upstreamId")
        if (future == null) {
            future = executor.scheduleAtFixedRate(
                {
                    val delay = System.currentTimeMillis() - lastHeadUpdated
                    delayed.set(delay > awaitHeadTimeoutMs)
                    if (delayed.get()) {
                        log.warn("No head updates $upstreamId for $delay ms @ ${this.javaClass.simpleName}")
                        onNoHeadUpdates()
                    }
                },
                180,
                30,
                TimeUnit.SECONDS,
            )
        }
    }
}
