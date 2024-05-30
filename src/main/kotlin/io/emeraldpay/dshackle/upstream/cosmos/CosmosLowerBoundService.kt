package io.emeraldpay.dshackle.upstream.cosmos

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType.STATE
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux

class CosmosLowerBoundService(
    chain: Chain,
    private val upstream: Upstream,
) : LowerBoundService(chain, upstream) {
    override fun detectors(): List<LowerBoundDetector> {
        return listOf(CosmosLowerBoundStateDetector(upstream))
    }
}

class CosmosLowerBoundStateDetector(
    private val upstream: Upstream,
) : LowerBoundDetector() {

    override fun period(): Long {
        return 3
    }

    override fun internalDetectLowerBound(): Flux<LowerBoundData> {
        return upstream.getIngressReader().read(ChainRequest("status", ListParams())).map {
            val resp = Global.objectMapper.readValue(it.getResult(), CosmosStatus::class.java)
            LowerBoundData(resp.syncInfo.earliestBlockHeight.toLong(), STATE)
        }.toFlux()
    }

    override fun types(): Set<LowerBoundType> {
        return setOf(STATE)
    }
}
