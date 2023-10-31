package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

typealias UpstreamValidatorBuilder = (Chain, Upstream, ChainOptions.Options, ChainConfig) -> UpstreamValidator?

interface UpstreamValidator {
    fun start(): Flux<UpstreamAvailability>

    fun validateUpstreamSettings(): Mono<ValidateUpstreamSettingsResult>

    fun validateUpstreamSettingsOnStartup(): ValidateUpstreamSettingsResult {
        return validateUpstreamSettings().block() ?: ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
    }
}

enum class ValidateUpstreamSettingsResult {
    UPSTREAM_VALID,
    UPSTREAM_SETTINGS_ERROR,
    UPSTREAM_FATAL_SETTINGS_ERROR,
}
