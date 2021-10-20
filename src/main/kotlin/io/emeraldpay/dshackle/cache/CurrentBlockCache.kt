package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.reader.Reader
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

class CurrentBlockCache<K, D> : Reader<K, D> {

    private val cache = AtomicReference(ConcurrentHashMap<K, D>())

    override fun read(key: K): Mono<D> {
        return Mono.justOrEmpty(cache.get()[key])
    }

    fun put(key: K, data: D) {
        cache.get()[key] = data
    }

    fun evict() {
        cache.set(ConcurrentHashMap())
    }
}
