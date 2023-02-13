package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.dshackle.upstream.ethereum.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnector
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse

class ConnectorFactoryMock implements ConnectorFactory {
    Reader<JsonRpcRequest, JsonRpcResponse> api
    Head head

    ConnectorFactoryMock(Reader<JsonRpcRequest, JsonRpcResponse> api, Head head) {
        this.api = api
        this.head = head
    }

    boolean isValid() {
        return true
    }

    EthereumConnector create(DefaultUpstream upstream, EthereumUpstreamValidator validator, Chain chain)  {
        return new EthereumConnectorMock(api, head)
    }
}