package io.emeraldpay.dshackle.upstream.solana

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService

class SolanaLowerBoundService(
    chain: Chain,
    private val upstream: Upstream,
) : LowerBoundService(chain, upstream) {
    override fun detectors(): List<LowerBoundDetector> {
        return listOf(SolanaLowerBoundSlotDetector(upstream))
    }
}
