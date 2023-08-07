package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.ethereum.EthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.EthereumRpcHead
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsConnectionPoolFactory
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsHead
import io.emeraldpay.dshackle.upstream.ethereum.NoEthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPool
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptionsImpl
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode.RPC_ONLY
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode.RPC_REQUESTS_WITH_MIXED_HEAD
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode.RPC_REQUESTS_WITH_WS_HEAD
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode.WS_ONLY
import io.emeraldpay.dshackle.upstream.forkchoice.AlwaysForkChoice
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import org.slf4j.LoggerFactory
import reactor.core.scheduler.Scheduler
import java.time.Duration

class EthereumRpcConnector(
    private val connectorType: ConnectorMode,
    private val directReader: JsonRpcReader,
    wsFactory: EthereumWsConnectionPoolFactory?,
    id: String,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator,
    skipEnhance: Boolean,
    wsConnectionResubscribeScheduler: Scheduler,
    headScheduler: Scheduler
) : EthereumConnector, CachesEnabled {
    private val pool: WsConnectionPool?
    private val head: Head

    companion object {
        private val log = LoggerFactory.getLogger(EthereumRpcConnector::class.java)
    }

    override fun getConnectorMode() = connectorType

    init {
        pool = wsFactory?.create(null)

        head = when (connectorType) {
            RPC_ONLY -> {
                log.warn("Setting up connector for $id upstream with RPC-only access, less effective than WS+RPC")
                EthereumRpcHead(getIngressReader(), forkChoice, id, blockValidator, headScheduler)
            }

            WS_ONLY -> {
                throw IllegalStateException("WS-only mode is not supported in RPC connector")
            }

            RPC_REQUESTS_WITH_MIXED_HEAD -> {
                val wsHead =
                    EthereumWsHead(
                        id,
                        AlwaysForkChoice(),
                        blockValidator,
                        getIngressReader(),
                        WsSubscriptionsImpl(pool!!),
                        skipEnhance,
                        wsConnectionResubscribeScheduler,
                        headScheduler
                    )
                // receive all new blocks through WebSockets, but also periodically verify with RPC in case if WS failed
                val rpcHead =
                    EthereumRpcHead(
                        getIngressReader(),
                        AlwaysForkChoice(),
                        id,
                        blockValidator,
                        headScheduler,
                        Duration.ofSeconds(30)
                    )
                MergedHead(listOf(rpcHead, wsHead), forkChoice, headScheduler, "Merged for $id")
            }

            RPC_REQUESTS_WITH_WS_HEAD -> {
                EthereumWsHead(
                    id,
                    AlwaysForkChoice(),
                    blockValidator, getIngressReader(),
                    WsSubscriptionsImpl(pool!!), skipEnhance, wsConnectionResubscribeScheduler,
                    headScheduler
                )
            }
        }
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
    }

    override fun getIngressReader(): JsonRpcReader {
        return directReader
    }

    override fun getIngressSubscription(): EthereumIngressSubscription {
        return NoEthereumIngressSubscription.DEFAULT
    }

    override fun getHead(): Head {
        return head
    }
}
