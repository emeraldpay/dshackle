package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
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

class EthereumLowerBoundStateDetector(
    private val upstream: Upstream,
) : LowerBoundDetector(upstream.getChain()) {
    private val recursiveLowerBound = RecursiveLowerBound(upstream, LowerBoundType.STATE, stateErrors, lowerBounds)

    companion object {
        val stateErrors = setOf(
            "No state available for block", // nethermind
            "missing trie node", // geth
            "header not found", // optimism, bsc, avalanche
            "Node state is pruned", // kava
            "is not available, lowest height is", // kava, cronos
            "State already discarded for", // moonriver, moonbeam
            "your node is running with state pruning", // fuse
            "failed to compute tipset state", // filecoin-calibration
            "bad tipset height", // filecoin-calibration
            "body not found for block",
            "request beyond head block",
            "block not found",
            "could not find block",
            "unknown block",
            "header for hash not found",
            "after last accepted block",
            "Version has either been pruned, or is for a future block", // cronos
            "no historical RPC is available for this historical", // optimism
            "historical backend error", // optimism
            "load state tree: failed to load state tree", // filecoin
            "purged for block", // erigon
            "No state data", // our own error if there is "null" in response
            "state is not available", // bsc also can return this error along with "header not found"
            "Block with such an ID is pruned", // zksync
            "state at block", // berachain, eth
            "unsupported block number", // arb
            "unexpected state root", // fantom
            "evm module does not exist on height", // sei
            "failed to load state at height", // 0g
            "no state found for block", // optimism
            "old data not available due", // eth
            "State not found for block", // rootstock
            "state does not maintain archive data", // fantom
            "Access to archival, debug, or trace data is not included in your current plan", // chainstack
            "empty reader set", // strange bsc geth error
        )
    }

    override fun period(): Long {
        return 3
    }

    override fun internalDetectLowerBound(): Flux<LowerBoundData> {
        return recursiveLowerBound.recursiveDetectLowerBound { block ->
            if (block == 0L) {
                Mono.just(ChainResponse(ByteArray(0), null))
            } else {
                upstream.getIngressReader().read(
                    ChainRequest(
                        "eth_getBalance",
                        ListParams(ZERO_ADDRESS, block.toHex()),
                    ),
                ).timeout(Defaults.internalCallsTimeout)
            }.doOnNext {
                if (it.hasResult() && it.getResult().contentEquals(Global.nullValue)) {
                    throw IllegalStateException("No state data")
                }
            }
        }.flatMap {
            Flux.just(it, lowerBoundFrom(it, LowerBoundType.TRACE))
        }
    }

    override fun types(): Set<LowerBoundType> {
        return setOf(LowerBoundType.STATE, LowerBoundType.TRACE)
    }
}
