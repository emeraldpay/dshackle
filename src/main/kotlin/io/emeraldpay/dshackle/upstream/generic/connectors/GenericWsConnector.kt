package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.EthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsHead
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessValidator
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPool
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPoolFactory
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptionsImpl
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.EthereumWsIngressSubscription
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.generic.ChainSpecific
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.time.Duration

class GenericWsConnector(
    wsFactory: WsConnectionPoolFactory,
    upstream: DefaultUpstream,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator,
    skipEnhance: Boolean,
    wsConnectionResubscribeScheduler: Scheduler,
    headScheduler: Scheduler,
    expectedBlockTime: Duration,
    chainSpecific: ChainSpecific,
) : GenericConnector {
    private val pool: WsConnectionPool
    private val reader: JsonRpcReader
    private val head: EthereumWsHead
    private val subscriptions: EthereumIngressSubscription
    private val liveness: HeadLivenessValidator
    init {
        pool = wsFactory.create(upstream)
        reader = JsonRpcWsClient(pool)
        val wsSubscriptions = WsSubscriptionsImpl(pool)
        head = EthereumWsHead(
            forkChoice,
            blockValidator,
            reader,
            wsSubscriptions,
            skipEnhance,
            wsConnectionResubscribeScheduler,
            headScheduler,
            upstream,
            chainSpecific,
        )
        liveness = HeadLivenessValidator(head, expectedBlockTime, headScheduler, upstream.getId())
        subscriptions = EthereumWsIngressSubscription(wsSubscriptions)
    }

    override fun hasLiveSubscriptionHead(): Flux<Boolean> {
        return liveness.getFlux()
    }
    override fun start() {
        pool.connect()
        head.start()
    }

    override fun isRunning(): Boolean {
        return head.isRunning()
    }

    override fun stop() {
        pool.close()
        head.stop()
    }

    override fun getIngressReader(): JsonRpcReader {
        return reader
    }

    override fun getIngressSubscription(): EthereumIngressSubscription {
        return subscriptions
    }

    override fun getHead(): Head {
        return head
    }
}
