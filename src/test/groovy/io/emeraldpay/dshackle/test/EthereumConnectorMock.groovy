package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnector
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse

class EthereumConnectorMock implements EthereumConnector {
    Reader<JsonRpcRequest, JsonRpcResponse> api
    Head head
    EthereumConnectorMock(Reader<JsonRpcRequest, JsonRpcResponse> api, Head head) {
        this.api = api
        this.head = head
    }

    @Override
    Reader<JsonRpcRequest, JsonRpcResponse> getApi() {
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
}