package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.ApiType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.HttpFactory
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import org.springframework.stereotype.Component
import java.net.URI

interface ConnectorFactoryCreator {
    fun createConnectorFactory(
        id: String,
        conn: UpstreamsConfig.RpcConnection,
        chain: Chain,
        forkChoice: ForkChoice,
        blockValidator: BlockValidator,
        chainsConf: ChainsConfig.ChainConfig,
    ): ConnectorFactory?

    fun buildHttpFactory(conn: UpstreamsConfig.HttpEndpoint?, urls: ArrayList<URI>? = null): HttpFactory?
}

@Component
class ConnectorFactoryCreatorResolver(
    private val genericConnectorFactoryCreator: GenericConnectorFactoryCreator,
    private val restConnectorFactoryCreator: RestConnectorFactoryCreator,
) {
    fun resolve(chain: Chain): ConnectorFactoryCreator {
        if (chain.type.apiType == ApiType.REST) {
            return restConnectorFactoryCreator
        }
        return genericConnectorFactoryCreator
    }
}
