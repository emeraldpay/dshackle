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

class EthereumConnectorMock implements EthereumConnector {
    Reader<JsonRpcRequest, JsonRpcResponse> api
    Head head
    ConnectorMode mode

    EthereumConnectorMock(Reader<JsonRpcRequest, JsonRpcResponse> api, Head head, ConnectorMode mode) {
        this.api = api
        this.mode = mode
        this.head = head
    }

    @Override
    ConnectorMode getConnectorMode() {
        return this.mode
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