package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectNewHeads
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux

open class EthereumSubscribe(
        val upstream: EthereumMultistream
) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumSubscribe::class.java)
    }

    private val newHeads = ConnectNewHeads(upstream)

    open fun subscribe(method: String, params: List<*>): Flux<out Any> {
        if (method == "newHeads") {
            return newHeads.connect()
        }
        return Flux.error(UnsupportedOperationException("Method $method is not supported"))
    }
}