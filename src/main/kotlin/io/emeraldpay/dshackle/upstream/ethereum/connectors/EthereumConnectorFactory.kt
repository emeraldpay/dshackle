package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.HttpFactory
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsConnectionPoolFactory
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode.RPC_ONLY
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode.RPC_REQUESTS_WITH_MIXED_HEAD
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode.RPC_REQUESTS_WITH_WS_HEAD
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode.WS_ONLY
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import reactor.core.scheduler.Scheduler
import java.time.Duration

open class EthereumConnectorFactory(
    private val connectorType: ConnectorMode,
    private val wsFactory: EthereumWsConnectionPoolFactory?,
    private val httpFactory: HttpFactory?,
    private val forkChoice: ForkChoice,
    private val blockValidator: BlockValidator,
    private val wsConnectionResubscribeScheduler: Scheduler,
    private val headScheduler: Scheduler,
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
        validator: EthereumUpstreamValidator,
        chain: Chain,
        skipEnhance: Boolean,
    ): EthereumConnector {
        if (wsFactory != null && connectorType == WS_ONLY) {
            return EthereumWsConnector(
                wsFactory,
                upstream,
                forkChoice,
                blockValidator,
                skipEnhance,
                wsConnectionResubscribeScheduler,
                headScheduler,
                expectedBlockTime,
            )
        }
        if (httpFactory == null) {
            throw java.lang.IllegalArgumentException("Can't create rpc connector if no http factory set")
        }
        return EthereumRpcConnector(
            connectorType,
            httpFactory.create(upstream.getId(), chain),
            wsFactory,
            upstream,
            forkChoice,
            blockValidator,
            skipEnhance,
            wsConnectionResubscribeScheduler,
            headScheduler,
            expectedBlockTime,
        )
    }

    enum class ConnectorMode {
        WS_ONLY,
        RPC_ONLY,
        RPC_REQUESTS_WITH_MIXED_HEAD,
        RPC_REQUESTS_WITH_WS_HEAD,
        ;

        companion object {
            val values = values().map { it.name }.toSet()
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
