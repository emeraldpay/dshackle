package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.generic.connectors.RestConnectorFactory
import org.springframework.stereotype.Component
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.net.URI

@Component
class RestConnectorFactoryCreator(
    fileResolver: FileResolver,
    private val headScheduler: Scheduler,
    private val headLivenessScheduler: Scheduler,
) : GenericConnectorFactoryCreator(
    fileResolver,
    Schedulers.single(),
    headScheduler,
    Schedulers.single(),
    headLivenessScheduler,
) {
    override fun createConnectorFactory(
        id: String,
        conn: UpstreamsConfig.RpcConnection,
        chain: Chain,
        forkChoice: ForkChoice,
        blockValidator: BlockValidator,
        chainsConf: ChainsConfig.ChainConfig,
    ): ConnectorFactory? {
        val urls = ArrayList<URI>()
        val httpFactory = buildHttpFactory(conn.rpc, urls)
        log.info("Using ${chain.chainName} upstream, at ${urls.joinToString()}")

        val connectorFactory =
            RestConnectorFactory(
                httpFactory,
                forkChoice,
                blockValidator,
                headScheduler,
                headLivenessScheduler,
                chainsConf.expectedBlockTime,
            )
        if (!connectorFactory.isValid()) {
            log.warn("Upstream configuration is invalid - no http endpoint")
            return null
        }
        return connectorFactory
    }
}
