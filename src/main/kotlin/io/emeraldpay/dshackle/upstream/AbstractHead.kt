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
import reactor.core.publisher.TopicProcessor
import java.util.concurrent.atomic.AtomicReference

abstract class AbstractHead : Head {

    companion object {
        private val log = LoggerFactory.getLogger(AbstractHead::class.java)
    }

    private val head = AtomicReference<BlockContainer>(null)
    private val stream: TopicProcessor<BlockContainer> = TopicProcessor.create()
    private val beforeBlockHandlers = ArrayList<Runnable>()

    fun follow(source: Flux<BlockContainer>): Disposable {
        return source
                .distinctUntilChanged {
                    it.hash
                }.filter { block ->
                    val curr = head.get()
                    curr == null || curr.difficulty < block.difficulty
                }
                .doFinally {
                    // close internal stream if upstream is finished, otherwise it gets stuck
                    // but technically is should never happen during normal work, only when the Head
                    // is stopping
                    stream.onComplete()
                }
                .subscribe { block ->
                    notifyBeforeBlock()
                    val prev = head.getAndUpdate { curr ->
                        if (curr == null || curr.difficulty < block.difficulty) {
                            block
                        } else {
                            curr
                        }
                    }
                    if (prev == null || prev.hash != block.hash) {
                        log.debug("New block ${block.height} ${block.hash}")
                        stream.onNext(block)
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
        return Flux.merge(
                Mono.justOrEmpty(head.get()),
                Flux.from(stream)
        ).onBackpressureLatest()
    }

    fun getCurrent(): BlockContainer? {
        return head.get()
    }

}