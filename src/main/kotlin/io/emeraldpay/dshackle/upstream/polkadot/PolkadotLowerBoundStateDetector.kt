package io.emeraldpay.dshackle.upstream.polkadot

import io.emeraldpay.dshackle.Defaults
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

class PolkadotLowerBoundStateDetector(
    private val upstream: Upstream,
) : LowerBoundDetector(upstream.getChain()) {
    private val recursiveLowerBound = RecursiveLowerBound(upstream, LowerBoundType.STATE, nonRetryableErrors, lowerBounds)

    companion object {
        private val nonRetryableErrors = setOf(
            "State already discarded for",
        )
    }

    override fun period(): Long {
        return 5
    }

    override fun internalDetectLowerBound(): Flux<LowerBoundData> {
        return recursiveLowerBound.recursiveDetectLowerBound { block ->
            upstream.getIngressReader().read(
                ChainRequest(
                    "chain_getBlockHash",
                    ListParams(block.toHex()), // in polkadot state methods work only with hash
                ),
            )
                .timeout(Defaults.internalCallsTimeout)
                .flatMap(ChainResponse::requireResult)
                .map {
                    String(it, 1, it.size - 2)
                }
                .flatMap {
                    upstream.getIngressReader().read(
                        ChainRequest(
                            "state_getMetadata",
                            ListParams(it),
                        ),
                    ).timeout(Defaults.internalCallsTimeout)
                }
        }
    }

    override fun types(): Set<LowerBoundType> {
        return setOf(LowerBoundType.STATE)
    }
}
