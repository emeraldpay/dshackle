package io.emeraldpay.dshackle.upstream.polkadot

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.RecursiveLowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import io.emeraldpay.dshackle.upstream.toHex
import reactor.core.publisher.Mono

class PolkadotLowerBoundBlockDetector(
    chain: Chain,
    private val upstream: Upstream,
) : RecursiveLowerBoundBlockDetector(chain, upstream) {

    companion object {
        private val nonRetryableErrors = setOf(
            "State already discarded for",
        )
    }

    override fun hasState(blockNumber: Long): Mono<Boolean> {
        return upstream.getIngressReader().read(
            ChainRequest(
                "chain_getBlockHash",
                ListParams(blockNumber.toHex()), // in polkadot state methods work only with hash
            ),
        )
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
                )
            }
            .retryWhen(retrySpec(nonRetryableErrors))
            .flatMap(ChainResponse::requireResult)
            .map { true }
            .onErrorReturn(false)
    }
}
