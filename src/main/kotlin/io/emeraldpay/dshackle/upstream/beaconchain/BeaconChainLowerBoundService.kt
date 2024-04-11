package io.emeraldpay.dshackle.upstream.beaconchain

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService

class BeaconChainLowerBoundService(
    chain: Chain,
    upstream: Upstream,
) : LowerBoundService(chain, upstream) {
    override fun detectors(): List<LowerBoundDetector> {
        return listOf(BeaconChainLowerBoundStateDetector())
    }
}
