package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.reader.JsonRpcHttpReader
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.ethereum.GenericWsHead
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessValidator
import io.emeraldpay.dshackle.upstream.ethereum.NoEthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPool
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPoolFactory
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
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.time.Duration

class GenericRpcConnector(
    connectorType: ConnectorMode,
    private val directReader: JsonRpcHttpReader,
    wsFactory: WsConnectionPoolFactory?,
    upstream: DefaultUpstream,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator,
    wsConnectionResubscribeScheduler: Scheduler,
    headScheduler: Scheduler,
    expectedBlockTime: Duration,
    chainSpecific: ChainSpecific,
) : GenericConnector, CachesEnabled {
    private val id = upstream.getId()
    private val pool: WsConnectionPool?
    private val head: Head
    private val liveness: HeadLivenessValidator

    companion object {
        private val log = LoggerFactory.getLogger(GenericRpcConnector::class.java)
    }

    override fun hasLiveSubscriptionHead(): Flux<Boolean> {
        return liveness.getFlux()
    }

    init {
        pool = wsFactory?.create(upstream)

        head = when (connectorType) {
            RPC_ONLY -> {
                log.warn("Setting up connector for $id upstream with RPC-only access, less effective than WS+RPC")
                GenericRpcHead(getIngressReader(), forkChoice, id, blockValidator, headScheduler, chainSpecific)
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
                        WsSubscriptionsImpl(pool!!),
                        wsConnectionResubscribeScheduler,
                        headScheduler,
                        upstream,
                        chainSpecific,
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
                    WsSubscriptionsImpl(pool!!),
                    wsConnectionResubscribeScheduler,
                    headScheduler,
                    upstream,
                    chainSpecific,
                )
            }
        }
        liveness = HeadLivenessValidator(head, expectedBlockTime, headScheduler, id)
    }

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

    override fun getIngressReader(): JsonRpcReader {
        return directReader
    }

    override fun getIngressSubscription(): IngressSubscription {
        return NoEthereumIngressSubscription.DEFAULT
    }

    override fun getHead(): Head {
        return head
    }
}
