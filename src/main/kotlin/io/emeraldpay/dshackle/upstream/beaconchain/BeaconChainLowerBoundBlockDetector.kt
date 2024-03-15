package io.emeraldpay.dshackle.upstream.beaconchain

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.LowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import reactor.core.publisher.Mono

class BeaconChainLowerBoundBlockDetector(
    chain: Chain,
    upstream: Upstream,
) : LowerBoundBlockDetector(chain, upstream) {

    // TODO: consensus nodes could be launched either in full mode or in archive mode
    override fun lowerBlockDetect(): Mono<LowerBlockData> {
        return Mono.just(LowerBlockData(1))
    }

    override fun periodRequest(): Long {
        return 120
    }
}
