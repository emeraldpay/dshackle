package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Flux

class EmptyEthereumHead : EthereumHead {

    override fun getFlux(): Flux<BlockJson<TransactionId>> {
        return Flux.empty()
    }
}