package io.emeraldpay.dshackle.upstream.lowerbound

import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

fun Long.toHex() = "0x${this.toString(16)}"

abstract class LowerBoundDetector {
    protected val log = LoggerFactory.getLogger(this::class.java)

    protected val lowerBounds = ConcurrentHashMap<LowerBoundType, LowerBoundData>()
    private val lowerBoundSink = Sinks.many().multicast().directBestEffort<LowerBoundData>()

    fun detectLowerBound(): Flux<LowerBoundData> {
        val notProcessing = AtomicBoolean(true)

        return Flux.merge(
            lowerBoundSink.asFlux(),
            Flux.interval(
                Duration.ofSeconds(15),
                Duration.ofMinutes(period()),
            )
                .filter { notProcessing.get() }
                .flatMap {
                    notProcessing.set(false)
                    internalDetectLowerBound()
                        .onErrorResume { Mono.just(LowerBoundData.default()) }
                        .switchIfEmpty(Flux.just(LowerBoundData.default()))
                        .doFinally { notProcessing.set(true) }
                },
        )
            .filter {
                it.lowerBound >= (lowerBounds[it.type]?.lowerBound ?: 0)
            }
            .map {
                lowerBounds[it.type] = it
                it
            }
    }

    // in minutes
    protected abstract fun period(): Long

    protected abstract fun internalDetectLowerBound(): Flux<LowerBoundData>

    abstract fun types(): Set<LowerBoundType>

    fun updateLowerBound(lowerBound: Long, type: LowerBoundType) {
        lowerBoundSink.emitNext(LowerBoundData(lowerBound, type)) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
    }
}
