package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.upstream.EthereumHead
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor

class EthereumHeadMock implements EthereumHead {

    private TopicProcessor<BlockJson<TransactionId>> bus = TopicProcessor.create()
    private BlockJson<TransactionId> latest

    void nextBlock(BlockJson<TransactionId> block) {
        assert block != null
        latest = block
        bus.onNext(block)
    }

    @Override
    Mono<BlockJson<TransactionId>> getHead() {
        return latest != null ? Mono.just(latest) : Mono.from(bus)
    }

    @Override
    Flux<BlockJson<TransactionId>> getFlux() {
        return Flux.concat(getHead(), bus).distinctUntilChanged()
    }
}
