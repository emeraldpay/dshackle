package io.emeraldpay.dshackle.upstream.lowerbound

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.Upstream
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.util.concurrent.ConcurrentHashMap

typealias LowerBoundServiceBuilder = (Chain, Upstream) -> LowerBoundService

abstract class LowerBoundService(
    private val chain: Chain,
    private val upstream: Upstream,
) {
    private val log = LoggerFactory.getLogger(this::class.java)

    private val lowerBounds = ConcurrentHashMap<LowerBoundType, LowerBoundData>()
    private val detectors: List<LowerBoundDetector> by lazy { detectors() }

    fun detectLowerBounds(): Flux<LowerBoundData> {
        return Flux.merge(
            detectors.map { it.detectLowerBound() },
        )
            .doOnNext {
                log.info("Lower bound of type ${it.type} is ${it.lowerBound} for upstream ${upstream.getId()} of chain $chain")
                lowerBounds[it.type] = it
            }
    }

    fun updateLowerBound(lowerBound: Long, type: LowerBoundType) {
        detectors
            .filter { it.types().contains(type) }
            .forEach { it.updateLowerBound(lowerBound, type) }
    }

    fun predictLowerBound(type: LowerBoundType): Long {
        return detectors
            .firstOrNull { it.types().contains(type) }
            ?.predictLowerBound(type)
            ?: 0
    }

    fun getLowerBounds(): Collection<LowerBoundData> = lowerBounds.values

    fun getLowerBound(lowerBoundType: LowerBoundType): LowerBoundData? = lowerBounds[lowerBoundType]

    protected abstract fun detectors(): List<LowerBoundDetector>
}
