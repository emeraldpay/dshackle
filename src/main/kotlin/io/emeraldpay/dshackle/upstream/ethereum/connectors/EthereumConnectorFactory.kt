package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.HttpFactory
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsFactory
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import org.slf4j.LoggerFactory

open class EthereumConnectorFactory(
    private val preferHttp: Boolean,
    private val wsFactory: EthereumWsFactory?,
    private val httpFactory: HttpFactory?,
    private val forkChoice: ForkChoice,
    private val blockValidator: BlockValidator
) : ConnectorFactory {
    companion object {
        private val log = LoggerFactory.getLogger(EthereumConnectorFactory::class.java)
    }

    override fun isValid(): Boolean {
        if (preferHttp && httpFactory == null) {
            return false
        }
        return true
    }

    override fun create(upstream: DefaultUpstream, validator: EthereumUpstreamValidator, chain: Chain): EthereumConnector {
        if (wsFactory != null && !preferHttp) {
            return EthereumWsConnector(wsFactory, upstream, validator, forkChoice, blockValidator)
        }
        if (httpFactory == null) {
            throw java.lang.IllegalArgumentException("Can't create rpc connector if no http factory set")
        }
        return EthereumRpcConnector(httpFactory.create(upstream.getId(), chain), wsFactory, upstream.getId(), forkChoice, blockValidator)
    }
}
