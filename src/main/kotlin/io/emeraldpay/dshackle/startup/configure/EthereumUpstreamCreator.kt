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
        var rating = 0
        val connection = if (upstreamsConfig.connection is UpstreamsConfig.EthereumPosConnection) {
            val posConn = upstreamsConfig.cast(UpstreamsConfig.EthereumPosConnection::class.java)
            rating = posConn.connection?.upstreamRating ?: 0
            posConn.connection?.execution
        } else {
            upstreamsConfig.cast(UpstreamsConfig.RpcConnection::class.java).connection
        }

        return buildGenericUpstream(
            upstreamsConfig.nodeId,
            upstreamsConfig,
            connection ?: throw IllegalStateException("Empty execution config"),
            chain,
            options,
            chainConf,
            rating,
        )
    }
}
