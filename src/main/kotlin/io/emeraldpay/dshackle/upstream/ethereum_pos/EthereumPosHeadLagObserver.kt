package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.DistanceExtractor
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HeadLagObserver
import io.emeraldpay.dshackle.upstream.Upstream
import org.slf4j.LoggerFactory

class EthereumPostHeadLagObserver(
    master: Head,
    followers: Collection<Upstream>
) : HeadLagObserver(master, followers, DistanceExtractor::extractPriorityDistance) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumPostHeadLagObserver::class.java)
    }

    override fun forkDistance(top: BlockContainer, curr: BlockContainer): Long {
        return 6
    }
}
