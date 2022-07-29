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
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toMono
import java.util.concurrent.atomic.AtomicReference

abstract class AbstractHead(
    private val forkChoice: ForkChoice
) : Head {

    companion object {
        private val log = LoggerFactory.getLogger(AbstractHead::class.java)
    }

    private var stream = Sinks.many().multicast().directBestEffort<BlockContainer>()
    private var completed = false
    private val beforeBlockHandlers = ArrayList<Runnable>()

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
                completed = true
                stream.tryEmitComplete()
            }
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe { block ->
                notifyBeforeBlock()
                when (val choiceResult = forkChoice.choose(block)) {
                    is ForkChoice.ChoiceResult.Updated -> {
                        val newHead = choiceResult.nwhead
                        log.debug("New block ${newHead.height} ${newHead.hash}")
                        val result = stream.tryEmitNext(newHead)
                        if (result.isFailure && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
                            log.warn("Failed to dispatch block: $result as ${this.javaClass}")
                        }
                    }
                    is ForkChoice.ChoiceResult.Same -> {}
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
        val curHead = forkChoice.getHead()
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
}
