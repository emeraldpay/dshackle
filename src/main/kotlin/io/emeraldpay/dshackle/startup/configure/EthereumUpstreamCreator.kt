package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.IndexConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import io.emeraldpay.dshackle.upstream.Upstream
import org.springframework.stereotype.Component

@Component
class EthereumUpstreamCreator(
    chainsConfig: ChainsConfig,
    indexConfig: IndexConfig,
    callTargets: CallTargetsHolder,
    genericConnectorFactoryCreator: ConnectorFactoryCreator,
) : GenericUpstreamCreator(chainsConfig, indexConfig, callTargets, genericConnectorFactoryCreator) {

    override fun createUpstream(
        upstreamsConfig: UpstreamsConfig.Upstream<*>,
        chain: Chain,
        options: ChainOptions.Options,
        chainConf: ChainsConfig.ChainConfig,
    ): Upstream? {
        val posConn = upstreamsConfig.cast(UpstreamsConfig.EthereumPosConnection::class.java)

        return buildGenericUpstream(
            upstreamsConfig.nodeId,
            upstreamsConfig,
            posConn.connection?.execution ?: throw IllegalStateException("Empty execution config"),
            chain,
            options,
            chainConf,
            posConn.connection?.upstreamRating ?: 0,
        )
    }
}
