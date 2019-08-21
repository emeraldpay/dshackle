package io.emeraldpay.dshackle.reader

import reactor.core.publisher.Mono

class EmptyReader<K, D>: Reader<K, D> {

    override fun read(key: K): Mono<D> {
        return Mono.empty()
    }
}