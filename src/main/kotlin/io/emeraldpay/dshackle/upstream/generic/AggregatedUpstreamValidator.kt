package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Defaults.Companion.internalCallsTimeout
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.SingleValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import reactor.core.publisher.Mono

class AggregatedUpstreamValidator(
    upstream: Upstream,
    options: ChainOptions.Options,
    private val validators: List<SingleValidator<UpstreamAvailability>>,
    private val startupValidators: List<SingleValidator<ValidateUpstreamSettingsResult>>,
) : UpstreamValidator(upstream, options) {

    override fun validate(): Mono<UpstreamAvailability> {
        return Mono.zip(
            validators.map { it.validate(UpstreamAvailability.UNAVAILABLE) },
        ) { a -> a.map { it as UpstreamAvailability } }
            .map(::resolve)
            .defaultIfEmpty(UpstreamAvailability.OK) // upstream is OK on case there are no validators
            .timeout(internalCallsTimeout)
            .onErrorResume {
                log.error("Error during upstream validation for {}, reason - {}", upstream.getId(), it.message)
                Mono.just(UpstreamAvailability.UNAVAILABLE)
            }
    }

    override fun validateUpstreamSettings(): Mono<ValidateUpstreamSettingsResult> {
        return Mono.zip(
            startupValidators.map { it.validate(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR) },
        ) { a -> a.map { it as ValidateUpstreamSettingsResult } }
            .map(::resolve)
            .defaultIfEmpty(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
            .timeout(internalCallsTimeout)
            .onErrorResume {
                log.error("Error during upstream validation for {}, reason - {}", upstream.getId(), it.message)
                Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR)
            }
    }

    override fun validateUpstreamSettingsOnStartup(): ValidateUpstreamSettingsResult {
        return runCatching {
            validateUpstreamSettings().block(internalCallsTimeout) ?: ValidateUpstreamSettingsResult.UPSTREAM_VALID
        }.getOrDefault(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR)
    }
}
