package io.emeraldpay.dshackle.upstream.near

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux

class NearUpstreamSettingsDetector(
    upstream: Upstream,
) : UpstreamSettingsDetector(upstream) {
    override fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.empty()
    }

    override fun clientVersionRequest(): ChainRequest {
        return ChainRequest("status", ListParams())
    }

    override fun parseClientVersion(data: ByteArray): String {
        return Global.objectMapper.readValue<NearVersionResponse>(data).nearVersion.version
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class NearVersionResponse(
        @JsonProperty("version")
        val nearVersion: NearVersion,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class NearVersion(
        @JsonProperty("version")
        val version: String,
    )
}
