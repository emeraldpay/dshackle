package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnector
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

    GenericConnector create(DefaultUpstream upstream, Chain chain)  {
        return new GenericConnectorMock(api, head)
    }
}