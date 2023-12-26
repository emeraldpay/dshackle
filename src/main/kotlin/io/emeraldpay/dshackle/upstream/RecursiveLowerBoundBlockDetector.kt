package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import reactor.core.publisher.Mono

abstract class RecursiveLowerBoundBlockDetector(
    chain: Chain,
    private val upstream: Upstream,
) : LowerBoundBlockDetector(chain, upstream) {

    override fun lowerBlockDetect(): Mono<LowerBlockData> {
        return Mono.just(upstream.getHead())
            .flatMap {
                val currentHeight = it.getCurrentHeight()
                if (currentHeight == null) {
                    Mono.empty()
                } else {
                    Mono.just(LowerBoundData(0, currentHeight))
                }
            }
            .expand { data ->
                if (data.found) {
                    Mono.empty()
                } else {
                    val middle = middleBlock(data)

                    if (data.left > data.right) {
                        val current = if (data.current == 0L) 1 else data.current
                        Mono.just(LowerBoundData(current, true))
                    } else {
                        hasState(middle)
                            .map {
                                if (it) {
                                    LowerBoundData(data.left, middle - 1, middle)
                                } else {
                                    LowerBoundData(middle + 1, data.right, data.current)
                                }
                            }
                    }
                }
            }
            .filter { it.found }
            .next()
            .map {
                LowerBlockData(it.current)
            }
    }

    private fun middleBlock(lowerBoundData: LowerBoundData): Long =
        lowerBoundData.left + (lowerBoundData.right - lowerBoundData.left) / 2

    protected abstract fun hasState(blockNumber: Long): Mono<Boolean>
}
