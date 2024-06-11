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

class EthereumLowerBoundLogsDetector(
    private val upstream: Upstream,
) : LowerBoundDetector() {

    companion object {
        const val MAX_OFFSET = 20
        private const val NO_LOGS_DATA = "No logs data"
    }

    private val recursiveLowerBound = RecursiveLowerBound(upstream, LowerBoundType.LOGS, setOf(NO_LOGS_DATA), lowerBounds)

    override fun period(): Long {
        return 3
    }

    override fun internalDetectLowerBound(): Flux<LowerBoundData> {
        return recursiveLowerBound.recursiveDetectLowerBoundWithOffset(MAX_OFFSET) { block ->
            upstream.getIngressReader()
                .read(
                    ChainRequest(
                        "eth_getLogs",
                        ListParams(
                            mapOf(
                                "fromBlock" to block.toHex(),
                                "toBlock" to block.toHex(),
                            ),
                        ),
                    ),
                )
                .doOnNext {
                    if (it.hasResult() && (it.getResult().contentEquals("null".toByteArray()) || it.getResult().contentEquals("[]".toByteArray()))) {
                        throw IllegalStateException(NO_LOGS_DATA)
                    }
                }
        }
    }

    override fun types(): Set<LowerBoundType> {
        return setOf(LowerBoundType.LOGS)
    }
}
