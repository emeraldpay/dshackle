package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.GenericWsHead
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessValidator
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPool
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPoolFactory
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptionsImpl
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
    wsConnectionResubscribeScheduler: Scheduler,
    headScheduler: Scheduler,
    headLivenessScheduler: Scheduler,
    expectedBlockTime: Duration,
    chainSpecific: ChainSpecific,
) : GenericConnector {
    private val pool: WsConnectionPool
    private val reader: ChainReader
    private val head: GenericWsHead
    private val subscriptions: IngressSubscription
    private val liveness: HeadLivenessValidator
    init {
        pool = wsFactory.create(upstream)
        reader = JsonRpcWsClient(pool)
        val wsSubscriptions = WsSubscriptionsImpl(pool)
        head = GenericWsHead(
            forkChoice,
            blockValidator,
            reader,
            wsSubscriptions,
            wsConnectionResubscribeScheduler,
            headScheduler,
            upstream,
            chainSpecific,
            expectedBlockTime,
        )
        liveness = HeadLivenessValidator(head, expectedBlockTime, headLivenessScheduler, upstream.getId())
        subscriptions = chainSpecific.makeIngressSubscription(wsSubscriptions)
    }

    override fun hasLiveSubscriptionHead(): Flux<Boolean> {
        return liveness.getFlux().distinctUntilChanged()
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

    override fun getIngressReader(): ChainReader {
        return reader
    }

    override fun getIngressSubscription(): IngressSubscription {
        return subscriptions
    }

    override fun getHead(): Head {
        return head
    }
}
