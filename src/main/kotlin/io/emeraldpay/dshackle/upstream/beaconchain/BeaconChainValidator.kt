package io.emeraldpay.dshackle.upstream.beaconchain

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.BasicEthUpstreamValidator
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import reactor.core.publisher.Mono
import java.util.concurrent.TimeoutException
import java.util.function.Supplier

class BeaconChainValidator(
    upstream: Upstream,
    options: ChainOptions.Options,
) : BasicEthUpstreamValidator(upstream, options) {

    override fun validatorFunctions(): List<Supplier<Mono<UpstreamAvailability>>> {
        return listOf(
            Supplier { validateSyncing() },
            Supplier { validateHealth() },
            Supplier { validatePeers() },
        )
    }

    override fun validateUpstreamSettings(): Mono<ValidateUpstreamSettingsResult> {
        return Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
    }

    private fun validateHealth(): Mono<UpstreamAvailability> {
        return upstream.getIngressReader()
            .read(ChainRequest("GET#/eth/v1/node/health", RestParams.emptyParams()))
            .flatMap(ChainResponse::requireResult)
            .map { UpstreamAvailability.OK }
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.warn("No response for /eth/v1/node/health from ${upstream.getId()}") }
                    .then(Mono.error(TimeoutException("Validation timeout for /eth/v1/node/health"))),
            )
            .doOnError { err -> log.error("Error during /eth/v1/node/health validation for ${upstream.getId()}", err) }
            .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    override fun validateSyncingRequest(): ValidateSyncingRequest {
        return ValidateSyncingRequest(
            ChainRequest("GET#/eth/v1/node/syncing", RestParams.emptyParams()),
        ) { bytes -> Global.objectMapper.readValue(bytes, BeaconChainSyncing::class.java).data.isSyncing }
    }

    override fun validatePeersRequest(): ValidatePeersRequest {
        return ValidatePeersRequest(
            ChainRequest("GET#/eth/v1/node/peer_count", RestParams.emptyParams()),
        ) { resp ->
            Global.objectMapper.readValue(
                resp.getResult(),
                BeaconChainPeers::class.java,
            ).data.connected.toInt()
        }
    }

    private data class BeaconChainSyncing(
        @JsonProperty("data")
        val data: BeaconChainSyncingData,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class BeaconChainSyncingData(
        @JsonProperty("is_syncing")
        val isSyncing: Boolean,
    )

    private data class BeaconChainPeers(
        @JsonProperty("data")
        val data: BeaconChainPeersData,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class BeaconChainPeersData(
        @JsonProperty("connected")
        val connected: String,
    )
}
