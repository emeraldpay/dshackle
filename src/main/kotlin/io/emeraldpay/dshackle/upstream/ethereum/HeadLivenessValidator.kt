package io.emeraldpay.dshackle.upstream.ethereum

import reactor.core.publisher.Flux

interface HeadLivenessValidator {
    fun getFlux(): Flux<Boolean>
}

class NoHeadLivenessValidator : HeadLivenessValidator {

    override fun getFlux(): Flux<Boolean> {
        return Flux.just(false)
    }
}

class AlwaysHeadLivenessValidator : HeadLivenessValidator {
    override fun getFlux(): Flux<Boolean> {
        return Flux.just(true)
    }
}
