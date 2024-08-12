package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessState
import io.emeraldpay.dshackle.upstream.ethereum.NoEthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import reactor.core.publisher.Flux

class GenericConnectorMock implements GenericConnector {
    Reader<ChainRequest, ChainResponse> api
    Head head
    Flux<HeadLivenessState> liveness

    GenericConnectorMock(Reader<ChainRequest, ChainResponse> api, Head head) {
        this.api = api
        this.head = head
        this.liveness = Flux.just(HeadLivenessState.NON_CONSECUTIVE)
    }

    @Override
    Flux<HeadLivenessState> headLivenessEvents() {
        return liveness
    }

    @Override
    Reader<ChainRequest, ChainResponse> getIngressReader() {
        return this.api
    }

    @Override
    Head getHead() {
        return this.head
    }

    @Override
    void start() {}

    @Override
    void stop() {}

    @Override
    boolean isRunning() {
        return true
    }

    @Override
    IngressSubscription getIngressSubscription() {
        return NoEthereumIngressSubscription.DEFAULT
    }
}