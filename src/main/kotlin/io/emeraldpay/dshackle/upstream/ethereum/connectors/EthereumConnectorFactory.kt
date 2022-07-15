package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.HttpFactory
import io.emeraldpay.dshackle.upstream.HttpRpcFactory
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsFactory
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory

open class EthereumConnectorFactory(
    private val preferHttp: Boolean,
    private val wsFactory: EthereumWsFactory?,
    private val httpFactory: HttpFactory?
): ConnectorFactory {
    private val log = LoggerFactory.getLogger(EthereumConnectorFactory::class.java)

    override fun isValid(): Boolean {
        if (preferHttp && httpFactory == null) {
            return false;
        }
        return true
    }

    override fun create(upstream: DefaultUpstream, validator: EthereumUpstreamValidator, chain: Chain): EthereumConnector {
        if (wsFactory!= null && !preferHttp) {
            return EthereumWsConnector(wsFactory, upstream, validator, chain)
        }
        if (httpFactory == null) {
            throw java.lang.IllegalArgumentException("Can't create rpc connector if no http factory set")
        }
        return EthereumRpcConnector(httpFactory.create(upstream.getId(), chain), wsFactory, upstream.getId())
    }
}