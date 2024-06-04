package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.lowerbound.detector.RecursiveLowerBound
import io.emeraldpay.dshackle.upstream.lowerbound.toHex
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux

const val MAX_OFFSET = 20

class EthereumLowerBoundTxDetector(
    private val upstream: Upstream,
) : LowerBoundDetector() {
    private val recursiveLowerBound = RecursiveLowerBound(upstream, LowerBoundType.TX, setOf("No tx data"), lowerBounds)

    override fun period(): Long {
        return 3
    }

    override fun internalDetectLowerBound(): Flux<LowerBoundData> {
        return recursiveLowerBound.recursiveDetectLowerBoundWithOffset(MAX_OFFSET) { block ->
            upstream.getIngressReader()
                .read(
                    ChainRequest(
                        "eth_getBlockTransactionCountByNumber",
                        ListParams(block.toHex()),
                    ),
                )
                .doOnNext {
                    if (it.hasResult() && (it.getResult().contentEquals("null".toByteArray()) || it.getResultAsProcessedString().substring(2).toLong(16) == 0L)) {
                        throw IllegalStateException("No tx data")
                    }
                }
        }
    }

    override fun types(): Set<LowerBoundType> {
        return setOf(LowerBoundType.TX)
    }
}
