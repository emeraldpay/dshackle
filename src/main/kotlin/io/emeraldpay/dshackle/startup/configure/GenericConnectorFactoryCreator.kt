package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.BasicHttpFactory
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.HttpFactory
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionFactory
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPoolFactory
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.scheduler.Scheduler
import java.net.URI

@Component
open class GenericConnectorFactoryCreator(
    private val fileResolver: FileResolver,
    private val wsConnectionResubscribeScheduler: Scheduler,
    private val headScheduler: Scheduler,
    private val wsScheduler: Scheduler,
    private val headLivenessScheduler: Scheduler,
) : ConnectorFactoryCreator {
    protected val log = LoggerFactory.getLogger(this::class.java)

    override fun createConnectorFactory(
        id: String,
        conn: UpstreamsConfig.RpcConnection,
        chain: Chain,
        forkChoice: ForkChoice,
        blockValidator: BlockValidator,
        chainsConf: ChainsConfig.ChainConfig,
    ): ConnectorFactory? {
        val urls = ArrayList<URI>()
        val wsFactoryApi = buildWsFactory(id, chain, conn, urls)
        val httpFactory = buildHttpFactory(conn.rpc, urls)
        log.info("Using ${chain.chainName} upstream, at ${urls.joinToString()}")
        val connectorFactory =
            GenericConnectorFactory(
                conn.resolveMode(),
                wsFactoryApi,
                httpFactory,
                forkChoice,
                blockValidator,
                wsConnectionResubscribeScheduler,
                headScheduler,
                headLivenessScheduler,
                chainsConf.expectedBlockTime,
            )
        if (!connectorFactory.isValid()) {
            log.warn("Upstream configuration is invalid (probably no http endpoint)")
            return null
        }
        return connectorFactory
    }

    override fun buildHttpFactory(conn: UpstreamsConfig.HttpEndpoint?, urls: ArrayList<URI>?): HttpFactory? {
        return conn?.let { endpoint ->
            val tls = conn.tls?.let { tls ->
                tls.ca?.let { ca ->
                    fileResolver.resolve(ca).readBytes()
                }
            }
            urls?.add(endpoint.url)
            BasicHttpFactory(endpoint.url.toString(), conn.basicAuth, tls)
        }
    }

    private fun buildWsFactory(
        id: String,
        chain: Chain,
        conn: UpstreamsConfig.RpcConnection,
        urls: ArrayList<URI>? = null,
    ): WsConnectionPoolFactory? {
        return conn.ws?.let { endpoint ->
            val wsConnectionFactory = WsConnectionFactory(
                id,
                chain,
                endpoint.url,
                endpoint.origin ?: URI("http://localhost"),
                wsScheduler,
            ).apply {
                config = endpoint
                basicAuth = endpoint.basicAuth
            }
            val wsApi = WsConnectionPoolFactory(
                id,
                endpoint.connections,
                wsConnectionFactory,
            )
            urls?.add(endpoint.url)
            wsApi
        }
    }
}
