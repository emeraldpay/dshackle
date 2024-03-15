package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse

class ConnectorFactoryMock implements ConnectorFactory {
    Reader<ChainRequest, ChainResponse> api
    Head head

    ConnectorFactoryMock(Reader<ChainRequest, ChainResponse> api, Head head) {
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