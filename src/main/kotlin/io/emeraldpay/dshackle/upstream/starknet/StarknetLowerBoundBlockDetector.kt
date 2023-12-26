package io.emeraldpay.dshackle.upstream.starknet

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.LowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import reactor.core.publisher.Mono

class StarknetLowerBoundBlockDetector(
    chain: Chain,
    upstream: Upstream,
) : LowerBoundBlockDetector(chain, upstream) {

    // for starknet we assume that all nodes are archive
    override fun lowerBlockDetect(): Mono<LowerBlockData> {
        return Mono.just(LowerBlockData(1))
    }
}
