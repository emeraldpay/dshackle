package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.lowerbound.detector.RecursiveLowerBound
import io.emeraldpay.dshackle.upstream.lowerbound.toHex
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class EthereumLowerBoundBlockDetector(
    private val upstream: Upstream,
) : LowerBoundDetector() {
    private val recursiveLowerBound = RecursiveLowerBound(upstream, LowerBoundType.BLOCK, setOf("No block data"), lowerBounds)

    override fun period(): Long {
        return 3
    }

    override fun internalDetectLowerBound(): Flux<LowerBoundData> {
        return recursiveLowerBound.recursiveDetectLowerBound { block ->
            if (block == 0L) {
                Mono.just(ChainResponse(ByteArray(0), null))
            } else {
                upstream.getIngressReader()
                    .read(
                        ChainRequest(
                            "eth_getBlockByNumber",
                            ListParams(block.toHex(), false),
                        ),
                    )
                    .doOnNext {
                        if (it.hasResult() && it.getResult().contentEquals("null".toByteArray())) {
                            throw IllegalStateException("No block data")
                        }
                    }
            }
        }
    }

    override fun types(): Set<LowerBoundType> {
        return setOf(LowerBoundType.BLOCK)
    }
}
