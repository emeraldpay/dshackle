package io.emeraldpay.dshackle.upstream.polkadot

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService

class PolkadotLowerBoundService(
    chain: Chain,
    private val upstream: Upstream,
) : LowerBoundService(chain, upstream) {

    override fun detectors(): List<LowerBoundDetector> {
        return listOf(PolkadotLowerBoundStateDetector(upstream))
    }
}
