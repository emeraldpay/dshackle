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
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

abstract class AbstractHead : Head {
    companion object {
        private val log = LoggerFactory.getLogger(AbstractHead::class.java)
    }

    private val head = AtomicReference<BlockContainer>(null)
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
            .onErrorResume { t ->
                log.warn("Failed to get update block ${t.message}")
                Mono.empty<BlockContainer>()
            }.distinctUntilChanged {
                it.hash
            }.doFinally {
                // close internal stream if upstream is finished, otherwise it gets stuck,
                // but technically it should never happen during normal work, only when the Head
                // is stopping
                completed = true
                stream.tryEmitComplete()
            }.subscribeOn(Schedulers.boundedElastic())
            .subscribe { block ->
                notifyBeforeBlock()
                head.set(block)
                log.debug("New block ${block.height} ${block.hash}")
                val result = stream.tryEmitNext(block)
                if (result.isFailure && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
                    log.warn("Failed to dispatch block: $result as ${this.javaClass}")
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

    override fun getFlux(): Flux<BlockContainer> =
        Flux
            .concat(
                Mono.justOrEmpty(head.get()),
                stream.asFlux(),
                // when the upstream makes a reconfiguration the head may be restarted,
                // i.e. `follow` can be called multiple times and create a new stream each time
                // in this case just continue with the new stream for all existing subscribers
                Mono
                    .fromCallable { log.warn("Restarting the Head...") }
                    .delaySubscription(Duration.ofMillis(100))
                    .thenMany { getFlux() },
            ).onBackpressureLatest()

    fun getCurrent(): BlockContainer? = head.get()

    override fun getCurrentHeight(): Long? = getCurrent()?.height
}
