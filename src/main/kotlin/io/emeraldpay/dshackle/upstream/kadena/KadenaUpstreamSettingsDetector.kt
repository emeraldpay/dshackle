package io.emeraldpay.dshackle.upstream.kadena

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.BasicUpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.NodeTypeRequest
import io.emeraldpay.dshackle.upstream.UNKNOWN_CLIENT_VERSION
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import reactor.core.publisher.Flux

class KadenaUpstreamSettingsDetector(
    upstream: Upstream,
) : BasicUpstreamSettingsDetector(upstream) {
    override fun internalDetectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
        )
    }

    override fun clientVersionRequest(): ChainRequest {
        return ChainRequest("GET#/cut", RestParams.emptyParams())
    }

    override fun parseClientVersion(data: ByteArray): String {
        return Global.objectMapper.readValue<KadenaHeader>(data).instance
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class KadenaHeader(
        @JsonProperty("instance") var instance: String,
    )

    override fun nodeTypeRequest(): NodeTypeRequest = NodeTypeRequest(clientVersionRequest())

    override fun clientType(node: JsonNode): String = "kadena"

    override fun clientVersion(node: JsonNode): String =
        node.get("version")?.get("version")?.asText() ?: UNKNOWN_CLIENT_VERSION
}
