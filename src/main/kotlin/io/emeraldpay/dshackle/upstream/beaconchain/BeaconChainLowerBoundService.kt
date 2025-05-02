package io.emeraldpay.dshackle.upstream.beaconchain

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService

class BeaconChainLowerBoundService(
    private val chain: Chain,
    private val upstream: Upstream,
) : LowerBoundService(chain, upstream) {
    override fun detectors(): List<LowerBoundDetector> {
        return listOf(
            BeaconChainLowerBoundBlockDetector(chain, upstream),
            BeaconChainLowerBoundEpochDetector(chain, upstream),
            BeaconChainLowerBoundStateDetector(chain, upstream),
            BeaconChainLowerBoundBlobDetector(chain, upstream),
        )
    }
}
