package io.emeraldpay.dshackle.upstream.beaconchain

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBounds
import io.emeraldpay.dshackle.upstream.lowerbound.detector.RecursiveLowerBound
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux

class EpochRecursiveLowerBound(
    upstream: Upstream,
    type: LowerBoundType,
    nonRetryableErrors: Set<String>,
    lowerBounds: LowerBounds,
) : RecursiveLowerBound(upstream, type, nonRetryableErrors, lowerBounds) {

    // similar to recursive lower bound, but range is adjusted as epoch range is 32x smaller than height
    override fun initialRange(): Mono<LowerBoundBinarySearchData> {
        return Mono.just(upstream.getHead())
            .flatMap {
                val currentHeight = it.getCurrentHeight()
                if (currentHeight == null) {
                    Mono.empty()
                } else if (lowerBounds.getLastBound(type) == null) {
                    Mono.just(LowerBoundBinarySearchData(0, currentHeight / 32)) // 1 epoch is 32 slots
                } else {
                    Mono.just(LowerBoundBinarySearchData(lowerBounds.getLastBound(type)!!.lowerBound, currentHeight / 32)) // 1 epoch is 32 slots
                }
            }
    }
}

class BeaconChainLowerBoundEpochDetector(
    private val chain: Chain,
    private val upstream: Upstream,
) : LowerBoundDetector(chain) {
    private val recursiveLowerBound = EpochRecursiveLowerBound(upstream, LowerBoundType.EPOCH, stateErrors, lowerBounds)

    companion object {
        const val MAX_OFFSET = 20
        val notFoundError = "NOT_FOUND:" // e.g. {"message":"NOT_FOUND: beacon block at slot 1086646","code":404}
        val notFoundError2 = "Could not get requested state"
        val notFoundError3 = "missing state" // "missing state at slot 11609023"
        val stateErrors = setOf(notFoundError, notFoundError2, notFoundError3)
    }

    override fun period(): Long {
        return 5
    }

    override fun internalDetectLowerBound(): Flux<LowerBoundData> {
        return recursiveLowerBound.recursiveDetectLowerBoundWithOffset(MAX_OFFSET) { slot ->
            val restParams = RestParams(listOf(), emptyList(), listOf(slot.toString()), "[\"1\"]".toByteArray())

            upstream.getIngressReader()
                .read(ChainRequest("POST#/eth/v1/beacon/rewards/attestations/*", restParams))
                .flatMap(ChainResponse::requireResult)
                .timeout(Defaults.internalCallsTimeout)
                .map {
                    parseHeadersResponse(it)
                }
        }.toFlux()
    }

    override fun types(): Set<LowerBoundType> {
        return setOf(LowerBoundType.EPOCH)
    }

    private fun parseHeadersResponse(data: ByteArray): ChainResponse {
        val node = Global.objectMapper.readValue<JsonNode>(data)
        if (node.get("code") != null && node.get("message") != null && node.get("code").textValue() == "404") {
            return ChainResponse(null, ChainCallError(node.get("code").asInt(), node.get("message").asText(), node.get("message").asText()))
        }

        val jsonData = node.get("data")
        if (jsonData != null) {
            val str = jsonData.toString()
            if (str.length >= 2) {
                return ChainResponse(str.toByteArray(), null)
            }
        }
        return ChainResponse(null, ChainCallError(404, notFoundError))
    }
}
