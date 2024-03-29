package io.emeraldpay.dshackle.upstream.solana

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux

class SolanaUpstreamSettingsDetector(
    upstream: Upstream,
) : UpstreamSettingsDetector(upstream) {
    override fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.empty()
    }

    override fun clientVersionRequest(): ChainRequest {
        return ChainRequest("getVersion", ListParams())
    }

    override fun parseClientVersion(data: ByteArray): String {
        return Global.objectMapper.readValue<SolanaVersion>(data).version
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class SolanaVersion(
        @JsonProperty("solana-core")
        val version: String,
    )
}
