package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.HttpFactory
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsConnectionPoolFactory
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice

open class EthereumConnectorFactory(
    private val preferHttp: Boolean,
    private val wsFactory: EthereumWsConnectionPoolFactory?,
    private val httpFactory: HttpFactory?,
    private val forkChoice: ForkChoice,
    private val blockValidator: BlockValidator,
) : ConnectorFactory {

    override fun isValid(): Boolean {
        if (preferHttp && httpFactory == null) {
            return false
        }
        return true
    }

    override fun create(
        upstream: DefaultUpstream,
        validator: EthereumUpstreamValidator,
        chain: Chain,
        skipEnhance: Boolean
    ): EthereumConnector {
        if (wsFactory != null && !preferHttp) {
            return EthereumWsConnector(wsFactory, upstream, forkChoice, blockValidator, skipEnhance)
        }
        if (httpFactory == null) {
            throw java.lang.IllegalArgumentException("Can't create rpc connector if no http factory set")
        }
        return EthereumRpcConnector(
            httpFactory.create(upstream.getId(), chain),
            wsFactory,
            upstream.getId(),
            forkChoice,
            blockValidator,
            skipEnhance
        )
    }
}
