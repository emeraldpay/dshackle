package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainCallUpstreamException
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
) : LowerBoundDetector(upstream.getChain()) {

    companion object {
        private const val NO_BLOCK_DATA = "No block data"

        private val NO_BLOCK_ERRORS = listOf(
            "error loading messages for tipset",
            "bad tipset height",
        )
    }

    private val recursiveLowerBound = RecursiveLowerBound(upstream, LowerBoundType.BLOCK, setOf(NO_BLOCK_DATA), lowerBounds)

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
                    .timeout(Defaults.internalCallsTimeout)
                    .doOnNext {
                        if (it.hasResult() && it.getResult().contentEquals("null".toByteArray())) {
                            throw IllegalStateException(NO_BLOCK_DATA)
                        }
                    }.doOnError {
                        if (it is ChainCallUpstreamException && handleError(it.error)) {
                            throw IllegalStateException(NO_BLOCK_DATA)
                        }
                    }
            }
        }.flatMap {
            Flux.just(it, lowerBoundFrom(it, LowerBoundType.LOGS))
        }
    }

    private fun handleError(error: ChainCallError): Boolean {
        return NO_BLOCK_ERRORS.any { error.message.contains(it) }
    }

    override fun types(): Set<LowerBoundType> {
        return setOf(LowerBoundType.BLOCK, LowerBoundType.LOGS)
    }
}
