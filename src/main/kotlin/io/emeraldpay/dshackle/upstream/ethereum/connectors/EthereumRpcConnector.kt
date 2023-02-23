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
import io.emeraldpay.dshackle.upstream.forkchoice.AlwaysForkChoice
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import org.slf4j.LoggerFactory
import java.time.Duration

class EthereumRpcConnector(
    private val directReader: JsonRpcReader,
    wsFactory: EthereumWsConnectionPoolFactory?,
    id: String,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator,
    skipEnhance: Boolean
) : EthereumConnector, CachesEnabled {
    private val pool: WsConnectionPool?
    private val head: Head

    companion object {
        private val log = LoggerFactory.getLogger(EthereumRpcConnector::class.java)
    }

    init {
        if (wsFactory != null) {
            // do not set upstream to the WS, since it doesn't control the RPC upstream
            pool = wsFactory.create(null)
            val subscriptions = WsSubscriptionsImpl(pool)
            val wsHead =
                EthereumWsHead(id, AlwaysForkChoice(), blockValidator, getIngressReader(), subscriptions, skipEnhance)
            // receive all new blocks through WebSockets, but also periodically verify with RPC in case if WS failed
            val rpcHead =
                EthereumRpcHead(getIngressReader(), AlwaysForkChoice(), id, blockValidator, Duration.ofSeconds(30))
            head = MergedHead(listOf(rpcHead, wsHead), forkChoice, "Merged for $id")
        } else {
            pool = null
            log.warn("Setting up connector for $id upstream with RPC-only access, less effective than WS+RPC")
            head = EthereumRpcHead(getIngressReader(), forkChoice, id, blockValidator)
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
