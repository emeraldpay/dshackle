package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
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
        return Flux.interval(
            Duration.ofSeconds(15),
            Duration.ofSeconds(60),
        )
            .flatMap { lowerBlockDetect() }
            .filter { it.blockNumber > currentLowerBlock.get().blockNumber }
            .map {
                log.info("Lower block of ${upstream.getId()} $chain: block height - {}, slot - {}", it.blockNumber, it.slot ?: "NA")

                currentLowerBlock.set(it)
                it
            }
    }

    fun getCurrentLowerBlock(): LowerBlockData = currentLowerBlock.get()

    protected abstract fun lowerBlockDetect(): Mono<LowerBlockData>

    data class LowerBlockData(
        val blockNumber: Long,
        val slot: Long?,
    ) : Comparable<LowerBlockData> {
        constructor(blockNumber: Long) : this(blockNumber, null)

        companion object {
            fun default() = LowerBlockData(0, 0)
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
