package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.data.BlockContainer
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.Function
import kotlin.concurrent.read
import kotlin.concurrent.write

class MergedPosHead(
    private val sources: Iterable<Pair<Int, Head>>
) : AbstractHead(), Lifecycle, CachesEnabled {

    companion object {
        private val log = LoggerFactory.getLogger(MergedPosHead::class.java)
    }
    private var subscription: Disposable? = null

    private val lock = ReentrantReadWriteLock()
    private val headLimit = 16
    private var head: List<Pair<Int, BlockContainer>> = emptyList()

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        sources.forEach {
            val head = it.second
            if (head is Lifecycle && !head.isRunning) {
                head.start()
            }
        }
        subscription?.dispose()
        subscription = super.follow(merge(sources.map { Pair(it.first, it.second.getFlux()) }))
    }

    fun merge(sources: Iterable<Pair<Int, Flux<BlockContainer>>>): Flux<BlockContainer> {
        return Flux.merge(
            sources.map {
                it.second.transform(process(it.first))
            }
        ).distinctUntilChanged {
            it.hash
        }
    }

    fun process(priority: Int): Function<Flux<BlockContainer>, Flux<BlockContainer>> {
        return Function { source ->
            source.handle { block, sink ->
                if (onNext(priority, block)) {
                    val top = lock.read {
                        head.lastOrNull()
                    }
                    if (top != null) {
                        sink.next(top.second)
                    }
                }
            }
        }
    }

    private fun onNext(priority: Int, block: BlockContainer): Boolean {
        val prev = lock.read {
            head.find { it.second.height == block.height }
        }
        if (prev != null && prev.first > priority) {
            return false
        }
        lock.write {
            // first, check if existing data for the height is better
            val prev = head.find { it.second.height == block.height }
            if (prev != null && prev.first > priority) {
                return false
            }

            // otherwise add it to the list
            val fresh = if (head.isEmpty()) {
                // just the first run, so nothing to do yet
                listOf(Pair(priority, block))
            } else if (head.last().second.height < block.height) {
                // new block, just add it on top
                head + Pair(priority, block)
            } else if (head.all { it.first < priority }) {
                // filled with low priority upstream that may be invalid, so replace the whole list
                listOf(Pair(priority, block))
            } else {
                // situation when we have that block in the list and since we did the checks above it can have only a lower priority
                // now there are two options: the same block or different block.
                // if it's in the middle keep the rest anyway b/c a higher priority upstream would fix it with the following updates
                head.map {
                    if (it.second.height == block.height) {
                        Pair(priority, block)
                    } else {
                        it
                    }
                }
            }
            head = fresh.takeLast(headLimit)
            return true
        }
    }

    override fun stop() {
        sources.forEach {
            val head = it.second
            if (head is Lifecycle && head.isRunning) {
                head.stop()
            }
        }
        subscription?.dispose()
        subscription = null
    }

    override fun setCaches(caches: Caches) {
        sources.forEach {
            val head = it.second
            if (head is CachesEnabled) {
                head.setCaches(caches)
            }
        }
    }
}
