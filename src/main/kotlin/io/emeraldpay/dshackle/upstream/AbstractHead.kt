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
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.core.publisher.Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER
import reactor.core.publisher.Sinks.EmitResult.OK
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toMono
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

abstract class AbstractHead(
    private val forkChoice: ForkChoice,
    private val blockValidator: BlockValidator = BlockValidator.ALWAYS_VALID,
    awaitHeadTimeoutMs: Long = 60_000
) : Head {

    companion object {
        private val log = LoggerFactory.getLogger(AbstractHead::class.java)
    }

    private var stream = Sinks.many().multicast().directBestEffort<BlockContainer>()
    private var completed = false
    private val beforeBlockHandlers = ArrayList<Runnable>()
    private var stopping = false
    private var lastHeadUpdated = 0L

    init {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
            {
                val delay = System.currentTimeMillis() - lastHeadUpdated
                if (delay > awaitHeadTimeoutMs) {
                    log.warn("No head updates for $delay ms @ ${this.javaClass} - restart")
                    start()
                }
            }, 300, 30, TimeUnit.SECONDS
        )
    }

    fun follow(source: Flux<BlockContainer>): Disposable {
        if (completed) {
            // if stream was already completed it cannot accept messages (with FAIL_TERMINATED), so needs to be recreated
            stream = Sinks.many().multicast().directBestEffort<BlockContainer>()
            completed = false
        }
        return source
            .distinctUntilChanged {
                it.hash
            }
            .filter { forkChoice.filter(it) }
            .doFinally {
                // close internal stream if upstream is finished, otherwise it gets stuck,
                // but technically it should never happen during normal work, only when the Head
                // is stopping
                if (stopping) {
                    log.info("Received signal $it - stop emit new head!!!")
                    completed = true
                    stream.tryEmitComplete()
                } else {
                    log.warn("Received signal $it unexpectedly - restart head")
                    start()
                }
            }
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe { block ->
                val valid = runCatching {
                    blockValidator.isValid(forkChoice.getHead(), block)
                }.onFailure {
                    log.error("Block ${block.hash} validation failed with '${it.message}'", it)
                }.getOrElse { false }
                if (valid) {
                    notifyBeforeBlock()
                    when (val choiceResult = forkChoice.choose(block)) {
                        is ForkChoice.ChoiceResult.Updated -> {
                            val newHead = choiceResult.nwhead
                            lastHeadUpdated = System.currentTimeMillis()
                            when (val result = stream.tryEmitNext(newHead)) {
                                OK -> log.debug("New block ${newHead.height} ${newHead.hash} @ ${this.javaClass}")
                                FAIL_ZERO_SUBSCRIBER -> log.debug("No subscribers for ${this.javaClass}")
                                else -> log.warn("Failed to dispatch block: $result as ${this.javaClass}")
                            }
                        }

                        is ForkChoice.ChoiceResult.Same -> {}
                    }
                } else {
                    log.warn("Invalid block $block}")
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
            stream.asFlux()
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
    }

    override fun start() {
        stopping = false
    }
}
