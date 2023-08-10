package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.EthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.NoEthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnector
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Flux

class EthereumConnectorMock implements EthereumConnector {
    Reader<JsonRpcRequest, JsonRpcResponse> api
    Head head
    Flux<Boolean> liveness

    EthereumConnectorMock(Reader<JsonRpcRequest, JsonRpcResponse> api, Head head) {
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
    EthereumIngressSubscription getIngressSubscription() {
        return NoEthereumIngressSubscription.DEFAULT
    }
}