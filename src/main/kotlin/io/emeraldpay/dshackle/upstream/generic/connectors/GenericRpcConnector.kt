package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HttpReader
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.NoIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.AlwaysHeadLivenessValidator
import io.emeraldpay.dshackle.upstream.ethereum.GenericWsHead
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessState
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessValidator
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessValidatorImpl
import io.emeraldpay.dshackle.upstream.ethereum.NoHeadLivenessValidator
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPool
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPoolFactory
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptionsImpl
import io.emeraldpay.dshackle.upstream.forkchoice.AlwaysForkChoice
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.generic.ChainSpecific
import io.emeraldpay.dshackle.upstream.generic.GenericRpcHead
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory.ConnectorMode
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory.ConnectorMode.RPC_ONLY
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory.ConnectorMode.RPC_REQUESTS_WITH_MIXED_HEAD
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory.ConnectorMode.RPC_REQUESTS_WITH_WS_HEAD
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory.ConnectorMode.WS_ONLY
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.time.Duration

class GenericRpcConnector(
    connectorType: ConnectorMode,
    private val directReader: HttpReader,
    wsFactory: WsConnectionPoolFactory?,
    upstream: DefaultUpstream,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator,
    wsConnectionResubscribeScheduler: Scheduler,
    headScheduler: Scheduler,
    headLivenessScheduler: Scheduler,
    expectedBlockTime: Duration,
    private val chainSpecific: ChainSpecific,
    private val chain: Chain,
) : GenericConnector, CachesEnabled {
    private val id = upstream.getId()
    private val pool: WsConnectionPool?
    private val wsSubs: WsSubscriptions?
    private val ingressSubscription: IngressSubscription?
    private val head: Head
    private val liveness: HeadLivenessValidator
    private val jsonRpcWsClient: JsonRpcWsClient?

    companion object {
        private val log = LoggerFactory.getLogger(GenericRpcConnector::class.java)
    }

    override fun headLivenessEvents(): Flux<HeadLivenessState> {
        return liveness.getFlux().distinctUntilChanged()
    }

    init {
        pool = wsFactory?.create(upstream)
        wsSubs = pool?.let { WsSubscriptionsImpl(it) }
        jsonRpcWsClient = pool?.let { JsonRpcWsClient(pool) }
        ingressSubscription = wsSubs?.let { chainSpecific.makeIngressSubscription(it) }

        head = when (connectorType) {
            RPC_ONLY -> {
                log.warn("Setting up connector for $id upstream with RPC-only access, less effective than WS+RPC")
                GenericRpcHead(
                    getIngressReader(),
                    forkChoice,
                    id,
                    blockValidator,
                    headScheduler,
                    chainSpecific,
                    expectedBlockTime.coerceAtLeast(Duration.ofSeconds(1)),
                )
            }

            WS_ONLY -> {
                throw IllegalStateException("WS-only mode is not supported in RPC connector")
            }

            RPC_REQUESTS_WITH_MIXED_HEAD -> {
                val wsHead =
                    GenericWsHead(
                        AlwaysForkChoice(),
                        blockValidator,
                        getIngressReader(),
                        wsSubs!!,
                        wsConnectionResubscribeScheduler,
                        headScheduler,
                        upstream,
                        chainSpecific,
                        jsonRpcWsClient!!,
                        expectedBlockTime,
                    )
                // receive all new blocks through WebSockets, but also periodically verify with RPC in case if WS failed
                val rpcHead =
                    GenericRpcHead(
                        getIngressReader(),
                        AlwaysForkChoice(),
                        id,
                        blockValidator,
                        headScheduler,
                        chainSpecific,
                        Duration.ofSeconds(30),
                    )
                MergedHead(listOf(rpcHead, wsHead), forkChoice, headScheduler, "Merged for $id")
            }

            RPC_REQUESTS_WITH_WS_HEAD -> {
                GenericWsHead(
                    AlwaysForkChoice(),
                    blockValidator,
                    getIngressReader(),
                    wsSubs!!,
                    wsConnectionResubscribeScheduler,
                    headScheduler,
                    upstream,
                    chainSpecific,
                    jsonRpcWsClient!!,
                    expectedBlockTime,
                )
            }
        }

        liveness = if (connectorType != RPC_ONLY && isSpecialChain(chain)) {
            AlwaysHeadLivenessValidator()
        } else {
            when (connectorType) {
                RPC_ONLY -> NoHeadLivenessValidator()
                RPC_REQUESTS_WITH_MIXED_HEAD, RPC_REQUESTS_WITH_WS_HEAD, WS_ONLY -> HeadLivenessValidatorImpl(head, expectedBlockTime, headLivenessScheduler, id)
            }
        }
    }

    private fun isSpecialChain(chain: Chain) =
        chain == Chain.OPEN_CAMPUS_CODEX__SEPOLIA || chain == Chain.ALEPHZERO__SEPOLIA || chain == Chain.EVERCLEAR__SEPOLIA || chain == Chain.ALEPHZERO__MAINNET

    override fun setCaches(caches: Caches) {
        if (head is CachesEnabled) {
            head.setCaches(caches)
        }
    }

    override fun start() {
        pool?.connect()
        if (head is Lifecycle) {
            head.start()
        }
    }

    override fun isRunning(): Boolean {
        if (head is Lifecycle) {
            return head.isRunning()
        }
        return true
    }

    override fun stop() {
        if (head is Lifecycle) {
            head.stop()
        }
        pool?.close()
        directReader.onStop()
    }

    override fun getIngressReader(): ChainReader {
        return directReader
    }

    override fun getIngressSubscription(): IngressSubscription {
        return ingressSubscription ?: NoIngressSubscription()
    }

    override fun getHead(): Head {
        return head
    }
}
