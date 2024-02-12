package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

typealias LowerBoundBlockDetectorBuilder = (Chain, Upstream) -> LowerBoundBlockDetector

fun Long.toHex() = "0x${this.toString(16)}"

abstract class LowerBoundBlockDetector(
    private val chain: Chain,
    private val upstream: Upstream,
) {
    private val currentLowerBlock = AtomicReference(LowerBlockData.default())

    protected val log = LoggerFactory.getLogger(this::class.java)

    fun lowerBlock(): Flux<LowerBlockData> {
        val notProcessing = AtomicBoolean(true)

        return Flux.interval(
            Duration.ofSeconds(15),
            Duration.ofMinutes(periodRequest()),
        )
            .filter { notProcessing.get() }
            .flatMap {
                notProcessing.set(false)
                lowerBlockDetect()
                    .onErrorResume { Mono.just(LowerBlockData.default()) }
                    .switchIfEmpty { Mono.just(LowerBlockData.default()) } // just to trigger onNext event
            }
            .doOnNext {
                notProcessing.set(true)
            }
            .filter { it.blockNumber > currentLowerBlock.get().blockNumber }
            .map {
                log.info("Lower block of ${upstream.getId()} $chain: block height - {}, slot - {}", it.blockNumber, it.slot ?: "NA")

                currentLowerBlock.set(it)
                it
            }
    }

    fun getCurrentLowerBlock(): LowerBlockData = currentLowerBlock.get()

    protected abstract fun lowerBlockDetect(): Mono<LowerBlockData>

    protected abstract fun periodRequest(): Long

    data class LowerBlockData(
        val blockNumber: Long,
        val slot: Long?,
        val timestamp: Long,
    ) : Comparable<LowerBlockData> {
        constructor(blockNumber: Long) : this(blockNumber, null, Instant.now().epochSecond)

        constructor(blockNumber: Long, slot: Long) : this(blockNumber, slot, Instant.now().epochSecond)

        companion object {
            fun default() = LowerBlockData(0, 0, 0)
        }

        override fun compareTo(other: LowerBlockData): Int {
            return this.blockNumber.compareTo(other.blockNumber)
        }
    }

    data class LowerBoundData(
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
