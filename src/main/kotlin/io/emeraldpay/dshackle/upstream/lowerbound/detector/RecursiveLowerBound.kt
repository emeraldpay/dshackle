package io.emeraldpay.dshackle.upstream.lowerbound.detector

import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.util.retry.Retry
import reactor.util.retry.RetryBackoffSpec
import java.time.Duration

class RecursiveLowerBound(
    private val upstream: Upstream,
    private val type: LowerBoundType,
    private val nonRetryableErrors: Set<String>,
) {
    private val log = LoggerFactory.getLogger(this::class.java)

    fun recursiveDetectLowerBound(hasData: (Long) -> Mono<ChainResponse>): Flux<LowerBoundData> {
        return Mono.just(upstream.getHead())
            .flatMap {
                val currentHeight = it.getCurrentHeight()
                if (currentHeight == null) {
                    Mono.empty()
                } else {
                    Mono.just(LowerBoundBinarySearch(0, currentHeight))
                }
            }
            .expand { data ->
                if (data.found) {
                    Mono.empty()
                } else {
                    val middle = middleBlock(data)

                    if (data.left > data.right) {
                        val current = if (data.current == 0L) 1 else data.current
                        Mono.just(LowerBoundBinarySearch(current, true))
                    } else {
                        hasData(middle)
                            .retryWhen(retrySpec(nonRetryableErrors))
                            .flatMap(ChainResponse::requireResult)
                            .map { true }
                            .onErrorReturn(false)
                            .map {
                                if (it) {
                                    LowerBoundBinarySearch(data.left, middle - 1, middle)
                                } else {
                                    LowerBoundBinarySearch(
                                        middle + 1,
                                        data.right,
                                        data.current,
                                    )
                                }
                            }
                    }
                }
            }
            .filter { it.found }
            .next()
            .map {
                LowerBoundData(it.current, type)
            }.toFlux()
    }

    private fun retrySpec(nonRetryableErrors: Set<String>): RetryBackoffSpec {
        return Retry.backoff(
            Long.MAX_VALUE,
            Duration.ofSeconds(1),
        )
            .maxBackoff(Duration.ofMinutes(3))
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

    private fun middleBlock(lowerBoundBinarySearch: LowerBoundBinarySearch): Long =
        lowerBoundBinarySearch.left + (lowerBoundBinarySearch.right - lowerBoundBinarySearch.left) / 2

    private data class LowerBoundBinarySearch(
        val left: Long,
        val right: Long,
        val current: Long,
        val found: Boolean,
    ) {
        constructor(left: Long, right: Long) : this(left, right, 0, false)

        constructor(left: Long, right: Long, current: Long) : this(left, right, current, false)

        constructor(current: Long, found: Boolean) : this(0, 0, current, found)
    }
}
