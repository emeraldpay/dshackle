package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.hot.CompatibleVersionsRules
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import org.springframework.stereotype.Component
import java.util.function.Supplier

@Component
class EthereumUpstreamCreator(
    chainsConfig: ChainsConfig,
    callTargets: CallTargetsHolder,
    connectorFactoryCreatorResolver: ConnectorFactoryCreatorResolver,
    versionRules: Supplier<CompatibleVersionsRules?>,
) : GenericUpstreamCreator(chainsConfig, callTargets, connectorFactoryCreatorResolver, versionRules) {

    override fun createUpstream(
        upstreamsConfig: UpstreamsConfig.Upstream<*>,
        chain: Chain,
        options: ChainOptions.Options,
        chainConf: ChainsConfig.ChainConfig,
    ): UpstreamCreationData {
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
