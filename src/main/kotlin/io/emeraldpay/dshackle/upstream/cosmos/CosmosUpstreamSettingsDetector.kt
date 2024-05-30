package io.emeraldpay.dshackle.upstream.cosmos

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.BasicUpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.NodeTypeRequest
import io.emeraldpay.dshackle.upstream.UNKNOWN_CLIENT_VERSION
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux

class CosmosUpstreamSettingsDetector(
    upstream: Upstream,
) : BasicUpstreamSettingsDetector(upstream) {
    override fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
        )
    }

    override fun clientVersionRequest(): ChainRequest {
        return ChainRequest("status", ListParams())
    }

    override fun parseClientVersion(data: ByteArray): String {
        return Global.objectMapper.readValue<CosmosStatus>(data).nodeInfo.version
    }

    override fun nodeTypeRequest(): NodeTypeRequest = NodeTypeRequest(clientVersionRequest())

    override fun clientType(node: JsonNode): String? = null

    override fun clientVersion(node: JsonNode): String? =
        node.get("node_info")?.get("version")?.asText() ?: UNKNOWN_CLIENT_VERSION
}
