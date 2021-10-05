package io.emeraldpay.dshackle.upstream.ethereum

import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux

open class EthereumSubscribe {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumSubscribe::class.java)
    }

    open fun subscribe(method: String, params: List<*>): Flux<Any> {
        return Flux.error(UnsupportedOperationException("Method $method is not supported"))
    }
}