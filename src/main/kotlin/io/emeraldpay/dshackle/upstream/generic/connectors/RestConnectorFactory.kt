package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.HttpFactory
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.generic.ChainSpecificRegistry
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Duration

class RestConnectorFactory(
    private val httpFactory: HttpFactory?,
    private val forkChoice: ForkChoice,
    private val blockValidator: BlockValidator,
    private val headScheduler: Scheduler,
    private val headLivenessScheduler: Scheduler,
    private val expectedBlockTime: Duration,
) : ConnectorFactory {

    override fun create(upstream: DefaultUpstream, chain: Chain): GenericConnector {
        val specific = ChainSpecificRegistry.resolve(chain)

        if (httpFactory == null) {
            throw IllegalArgumentException("No http endpoint")
        }

        return GenericRpcConnector(
            GenericConnectorFactory.ConnectorMode.RPC_ONLY,
            httpFactory.create(upstream.getId(), chain),
            null,
            upstream,
            forkChoice,
            blockValidator,
            Schedulers.single(),
            headScheduler,
            headLivenessScheduler,
            expectedBlockTime,
            specific,
            chain,
        )
    }

    override fun isValid(): Boolean {
        return httpFactory != null
    }
}
