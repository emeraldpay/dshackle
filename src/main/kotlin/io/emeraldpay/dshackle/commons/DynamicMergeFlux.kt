package io.emeraldpay.dshackle.commons

import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import java.util.concurrent.ConcurrentHashMap

class DynamicMergeFlux<K : Any, T>(private val scheduler: Scheduler) {

    private val merge = Sinks.many().multicast().directBestEffort<T>()
    private val sources = ConcurrentHashMap<K, Disposable>()

    fun add(flux: Flux<T>, id: K) {
        remove(id)
        sources.computeIfAbsent(id) { _ ->
            flux.subscribeOn(scheduler).subscribe {
                merge.emitNext(it) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
            }
        }
    }

    fun remove(id: K) {
        sources.remove(id)?.dispose()
    }

    fun asFlux(): Flux<T> = merge.asFlux()

    fun stop() {
        sources.forEach { (_, d) -> d.dispose() }
        merge.emitComplete { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
    }

    fun getKeys(): List<K> = sources.keys().toList()
}
