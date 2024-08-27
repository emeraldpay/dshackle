package io.emeraldpay.dshackle.upstream.beaconchain

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.BasicEthUpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.NodeTypeRequest
import io.emeraldpay.dshackle.upstream.UNKNOWN_CLIENT_VERSION
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import reactor.core.publisher.Flux

class BeaconChainUpstreamSettingsDetector(
    upstream: Upstream,
) : BasicEthUpstreamSettingsDetector(upstream) {

    override fun nodeTypeRequest(): NodeTypeRequest {
        return NodeTypeRequest(
            clientVersionRequest(),
        )
    }

    override fun internalDetectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
        )
    }

    override fun mapping(node: JsonNode): String {
        return node.get("data")?.get("version")?.asText() ?: ""
    }

    override fun clientVersionRequest(): ChainRequest {
        return ChainRequest("GET#/eth/v1/node/version", RestParams.emptyParams())
    }

    override fun parseClientVersion(data: ByteArray): String {
        val node = Global.objectMapper.readValue<JsonNode>(data)

        return node.get("data")?.get("version")?.textValue() ?: UNKNOWN_CLIENT_VERSION
    }
}
