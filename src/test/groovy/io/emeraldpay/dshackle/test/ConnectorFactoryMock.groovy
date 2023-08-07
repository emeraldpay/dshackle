package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.dshackle.upstream.ethereum.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnector
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse

class ConnectorFactoryMock implements ConnectorFactory {
    Reader<JsonRpcRequest, JsonRpcResponse> api
    Head head
    ConnectorMode mode

    ConnectorFactoryMock(Reader<JsonRpcRequest, JsonRpcResponse> api, Head head) {
        this(api, head, ConnectorMode.RPC_REQUESTS_WITH_WS_HEAD)
    }

    ConnectorFactoryMock(Reader<JsonRpcRequest, JsonRpcResponse> api, Head head, ConnectorMode mode) {
        this.api = api
        this.head = head
        this.mode = mode
    }

    boolean isValid() {
        return true
    }

    EthereumConnector create(DefaultUpstream upstream, EthereumUpstreamValidator validator, Chain chain, boolean skipEnhance)  {
        return new EthereumConnectorMock(api, head, this.mode)
    }
}