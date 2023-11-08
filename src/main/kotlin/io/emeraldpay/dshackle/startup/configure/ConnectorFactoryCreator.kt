package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.HttpRpcFactory
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import java.net.URI

interface ConnectorFactoryCreator {
    fun createConnectorFactoryCreator(
        id: String,
        conn: UpstreamsConfig.RpcConnection,
        chain: Chain,
        forkChoice: ForkChoice,
        blockValidator: BlockValidator,
        chainsConf: ChainsConfig.ChainConfig,
    ): ConnectorFactory?

    fun buildHttpFactory(conn: UpstreamsConfig.HttpEndpoint?, urls: ArrayList<URI>? = null): HttpRpcFactory?
}
