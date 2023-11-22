package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.IndexConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcHead
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcUpstream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinZMQHead
import io.emeraldpay.dshackle.upstream.bitcoin.EsploraClient
import io.emeraldpay.dshackle.upstream.bitcoin.ExtractBlock
import io.emeraldpay.dshackle.upstream.bitcoin.ZMQServer
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import org.springframework.stereotype.Component
import reactor.core.scheduler.Scheduler
import java.util.concurrent.atomic.AtomicInteger

@Component
class BitcoinUpstreamCreator(
    chainsConfig: ChainsConfig,
    indexConfig: IndexConfig,
    callTargets: CallTargetsHolder,
    private val genericConnectorFactoryCreator: ConnectorFactoryCreator,
    private val fileResolver: FileResolver,
    private val headScheduler: Scheduler,
) : UpstreamCreator(chainsConfig, indexConfig, callTargets) {
    private var seq = AtomicInteger(0)

    override fun createUpstream(
        upstreamsConfig: UpstreamsConfig.Upstream<*>,
        chain: Chain,
        options: ChainOptions.Options,
        chainConf: ChainsConfig.ChainConfig,
    ): Upstream? {
        val config = upstreamsConfig.cast(UpstreamsConfig.BitcoinConnection::class.java)
        val conn = config.connection!!
        val httpFactory = genericConnectorFactoryCreator.buildHttpFactory(conn.rpc)
        if (httpFactory == null) {
            log.warn("Upstream doesn't have API configuration")
            return null
        }
        val directApi = httpFactory.create(config.id, chain)
        val esplora = conn.esplora?.let { endpoint ->
            val tls = endpoint.tls?.let { tls ->
                tls.ca?.let { ca ->
                    fileResolver.resolve(ca).readBytes()
                }
            }
            EsploraClient(endpoint.url, endpoint.basicAuth, tls)
        }

        val extractBlock = ExtractBlock()
        val rpcHead = BitcoinRpcHead(directApi, extractBlock, headScheduler = headScheduler)
        val head: Head = conn.zeroMq?.let { zeroMq ->
            val server = ZMQServer(zeroMq.host, zeroMq.port, "hashblock")
            val zeroMqHead = BitcoinZMQHead(server, directApi, extractBlock, headScheduler)
            MergedHead(listOf(rpcHead, zeroMqHead), MostWorkForkChoice(), headScheduler)
        } ?: rpcHead

        val methods = buildMethods(config, chain)
        val upstream = BitcoinRpcUpstream(
            config.id
                ?: "bitcoin-${seq.getAndIncrement()}",
            chain, directApi, head,
            options, config.role,
            QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(config.labels)),
            methods, esplora, chainConf,
        )
        upstream.start()
        return upstream
    }
}
