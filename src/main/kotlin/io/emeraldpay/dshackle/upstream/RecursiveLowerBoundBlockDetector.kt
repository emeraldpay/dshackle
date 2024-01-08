package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import reactor.util.retry.RetryBackoffSpec
import java.time.Duration

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

    protected fun retrySpec(nonRetryableErrors: Set<String>): RetryBackoffSpec {
        return Retry.backoff(
            Long.MAX_VALUE,
            Duration.ofSeconds(1),
        )
            .maxBackoff(Duration.ofSeconds(3))
            .filter {
                !nonRetryableErrors.any { err -> it.message?.contains(err, true) ?: false }
            }
            .doAfterRetry {
                log.debug(
                    "Error in calculation of lower block of upstream {}, retry attempt - {}, message - {}",
                    upstream.getId(),
                    it.totalRetries(),
                    it.failure().message,
                )
            }
    }

    protected abstract fun hasState(blockNumber: Long): Mono<Boolean>
}
