package io.emeraldpay.dshackle.upstream

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface Head<T> {

    fun getHead(): Mono<T>

    fun getFlux(): Flux<T>
}