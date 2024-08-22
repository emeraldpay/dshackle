package io.emeraldpay.dshackle.upstream.beaconchain

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import reactor.core.publisher.Flux

class BeaconChainLowerBoundStateDetector(
    private val chain: Chain,
) : LowerBoundDetector(chain) {

    override fun period(): Long {
        return 120
    }

    override fun internalDetectLowerBound(): Flux<LowerBoundData> {
        return Flux.just(LowerBoundData(1, LowerBoundType.STATE))
    }

    override fun types(): Set<LowerBoundType> {
        return setOf(LowerBoundType.STATE)
    }
}
