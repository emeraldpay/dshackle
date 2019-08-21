package io.emeraldpay.dshackle.reader

import reactor.core.publisher.Mono

class CompoundReader<K, D>(
        private val readers: Collection<Reader<K, D>>
): Reader<K, D> {

    override fun read(key: K): Mono<D> {
        if (readers.isEmpty()) {
            return Mono.empty()
        }
        var result = readers.first().read(key)
        if (readers.size == 1) {
            return result
        }
        readers.stream().skip(1).forEach {
            result = result.switchIfEmpty(it.read(key))
        }
        return result
    }

}