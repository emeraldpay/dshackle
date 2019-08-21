package io.emeraldpay.dshackle.reader

import reactor.core.publisher.Mono

interface Reader<K, D> {

    fun read(key: K): Mono<D>

}