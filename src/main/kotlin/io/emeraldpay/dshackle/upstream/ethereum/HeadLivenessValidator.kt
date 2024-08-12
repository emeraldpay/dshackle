package io.emeraldpay.dshackle.upstream.ethereum

import reactor.core.publisher.Flux

enum class HeadLivenessState {
    OK, NON_CONSECUTIVE, DISCONNECTED, FATAL_ERROR
}

interface HeadLivenessValidator {
    fun getFlux(): Flux<HeadLivenessState>
}

class NoHeadLivenessValidator : HeadLivenessValidator {

    override fun getFlux(): Flux<HeadLivenessState> {
        return Flux.just(HeadLivenessState.NON_CONSECUTIVE)
    }
}

class AlwaysHeadLivenessValidator : HeadLivenessValidator {
    override fun getFlux(): Flux<HeadLivenessState> {
        return Flux.just(HeadLivenessState.OK)
    }
}
