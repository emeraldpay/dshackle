package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.NoEthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnector
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Flux

class GenericConnectorMock implements GenericConnector {
    Reader<JsonRpcRequest, JsonRpcResponse> api
    Head head
    Flux<Boolean> liveness

    GenericConnectorMock(Reader<JsonRpcRequest, JsonRpcResponse> api, Head head) {
        this.api = api
        this.head = head
        this.liveness = Flux.just(false)
    }

    @Override
    Flux<Boolean> hasLiveSubscriptionHead() {
        return liveness
    }

    @Override
    Reader<JsonRpcRequest, JsonRpcResponse> getIngressReader() {
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