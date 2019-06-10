package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference

class EthereumWsHead(
        private val ws: EthereumWs
): EthereumHead {

    private val head = AtomicReference<BlockJson<TransactionId>>(null)
    private val stream: Flux<BlockJson<TransactionId>> = ws.getFlux()

    override fun getHead(): Mono<BlockJson<TransactionId>> {
        val current = head.get()
        if (current != null) {
            return Mono.just(current)
        }
        return Mono.from(stream)
    }

    override fun getFlux(): Flux<BlockJson<TransactionId>> {
        return Flux.from(stream)
    }

}