package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.DistanceExtractor
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HeadLagObserver
import io.emeraldpay.dshackle.upstream.Upstream
import org.slf4j.LoggerFactory
import reactor.core.scheduler.Scheduler

class EthereumPosHeadLagObserver(
    master: Head,
    followers: Collection<Upstream>,
    headScheduler: Scheduler
) : HeadLagObserver(master, followers, DistanceExtractor::extractPriorityDistance, headScheduler) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumPosHeadLagObserver::class.java)
    }

    override fun forkDistance(top: BlockContainer, curr: BlockContainer): Long {
        return 6
    }
}
