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

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.data.BlockContainer
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.Function
import kotlin.concurrent.read
import kotlin.concurrent.write

class MergedPowHead(
    private val sources: Iterable<Head>,
) : AbstractHead(),
    Lifecycle,
    CachesEnabled {
    private var subscription: Disposable? = null

    private val lock = ReentrantReadWriteLock()
    private val headLimit = 16
    private var head: List<BlockContainer> = emptyList()

    override fun isRunning(): Boolean = subscription != null

    override fun start() {
        sources.forEach { head ->
            if (head is Lifecycle && !head.isRunning) {
                head.start()
            }
        }
        subscription?.dispose()
        subscription = super.follow(merge(sources.map { it.getFlux() }))
    }

    fun merge(sources: Iterable<Flux<BlockContainer>>): Flux<BlockContainer> =
        Flux
            .merge(
                sources.map {
                    it.transform(process())
                },
            ).distinctUntilChanged {
                it.hash
            }

    fun process(): Function<Flux<BlockContainer>, Flux<BlockContainer>> =
        Function { source ->
            source.handle { block, sink ->
                if (onNext(block)) {
                    val top =
                        lock.read {
                            head.lastOrNull()
                        }
                    if (top != null) {
                        sink.next(top)
                    }
                }
            }
        }

    private fun onNext(block: BlockContainer): Boolean {
        val prev =
            lock.read {
                head.find { it.height == block.height }
            }
        if (prev != null && prev.difficulty > block.difficulty) {
            return false
        }
        lock.write {
            // first, check if existing data for the height is better
            val prev = head.find { it.height == block.height }
            if (prev != null && prev.difficulty > block.difficulty) {
                return false
            }

            // otherwise add it to the list
            val fresh =
                if (head.isEmpty()) {
                    // just the first run, so nothing to do yet
                    listOf(block)
                } else if (head.last().height < block.height) {
                    // new block, just add it on top
                    head + block
                } else {
                    // situation when we have that block in the list and since we checked it above it has a lower priority
                    // now there are two options: the same block or different block.
                    // if it's in the middle keep the rest anyway b/c a higher priority upstream would fix it with the following updates
                    head.map {
                        if (it.height == block.height) {
                            block
                        } else {
                            it
                        }
                    }
                }
            head =
                fresh
                    // drop all blocks on top of this one if their difficulty is lower
                    .filterNot { it.height > block.height && it.difficulty < block.difficulty }
                    .takeLast(headLimit)
            return true
        }
    }

    override fun stop() {
        sources.forEach { head ->
            if (head is Lifecycle && head.isRunning) {
                head.stop()
            }
        }
        subscription?.dispose()
        subscription = null
    }

    override fun setCaches(caches: Caches) {
        sources.forEach {
            if (it is CachesEnabled) {
                it.setCaches(caches)
            }
        }
    }
}
