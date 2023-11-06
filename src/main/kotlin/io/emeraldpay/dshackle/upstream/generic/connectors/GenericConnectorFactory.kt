package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.HttpFactory
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPoolFactory
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.generic.ChainSpecificRegistry
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory.ConnectorMode.RPC_ONLY
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory.ConnectorMode.RPC_REQUESTS_WITH_MIXED_HEAD
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory.ConnectorMode.RPC_REQUESTS_WITH_WS_HEAD
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory.ConnectorMode.WS_ONLY
import reactor.core.scheduler.Scheduler
import java.time.Duration

open class GenericConnectorFactory(
    private val connectorType: ConnectorMode,
    private val wsFactory: WsConnectionPoolFactory?,
    private val httpFactory: HttpFactory?,
    private val forkChoice: ForkChoice,
    private val blockValidator: BlockValidator,
    private val wsConnectionResubscribeScheduler: Scheduler,
    private val headScheduler: Scheduler,
    private val headLivenessScheduler: Scheduler,
    private val expectedBlockTime: Duration,
) : ConnectorFactory {

    override fun isValid(): Boolean {
        if ((
            connectorType == RPC_ONLY ||
                connectorType == RPC_REQUESTS_WITH_MIXED_HEAD ||
                connectorType == RPC_REQUESTS_WITH_WS_HEAD
            ) && httpFactory == null
        ) {
            return false
        }
        if ((
            connectorType == WS_ONLY ||
                connectorType == RPC_REQUESTS_WITH_MIXED_HEAD ||
                connectorType == RPC_REQUESTS_WITH_WS_HEAD
            ) && wsFactory == null
        ) {
            return false
        }
        return true
    }

    override fun create(
        upstream: DefaultUpstream,
        chain: Chain,
    ): GenericConnector {
        val specific = ChainSpecificRegistry.resolve(chain)
        if (wsFactory != null && connectorType == WS_ONLY) {
            return GenericWsConnector(
                wsFactory,
                upstream,
                forkChoice,
                blockValidator,
                wsConnectionResubscribeScheduler,
                headScheduler,
                headLivenessScheduler,
                expectedBlockTime,
                specific,
            )
        }
        if (httpFactory == null) {
            throw java.lang.IllegalArgumentException("Can't create rpc connector if no http factory set")
        }
        return GenericRpcConnector(
            connectorType,
            httpFactory.create(upstream.getId(), chain),
            wsFactory,
            upstream,
            forkChoice,
            blockValidator,
            wsConnectionResubscribeScheduler,
            headScheduler,
            headLivenessScheduler,
            expectedBlockTime,
            specific,
        )
    }

    enum class ConnectorMode {
        WS_ONLY,
        RPC_ONLY,
        RPC_REQUESTS_WITH_MIXED_HEAD,
        RPC_REQUESTS_WITH_WS_HEAD,
        ;

        companion object {
            val values = entries.map { it.name }.toSet()
            fun parse(value: String): ConnectorMode {
                val upper = value.uppercase()
                if (!values.contains(upper)) {
                    throw IllegalArgumentException("Invalid connector mode: $value")
                }
                return valueOf(upper)
            }
        }
    }
}
