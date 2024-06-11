package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.SingleCallValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import reactor.core.publisher.Mono
import java.util.concurrent.TimeoutException

class GenericUpstreamValidator(
    upstream: Upstream,
    options: ChainOptions.Options,
    private val validators: List<SingleCallValidator<UpstreamAvailability>>,
    private val startupValidators: List<SingleCallValidator<ValidateUpstreamSettingsResult>>,
) : UpstreamValidator(upstream, options) {

    override fun validate(): Mono<UpstreamAvailability> {
        return Mono.zip(
            validators.map { exec(it, UpstreamAvailability.UNAVAILABLE) },
        ) { a -> a.map { it as UpstreamAvailability } }
            .map(::resolve)
            .defaultIfEmpty(UpstreamAvailability.UNAVAILABLE)
            .onErrorResume {
                log.error("Error during upstream validation for ${upstream.getId()}", it)
                Mono.just(UpstreamAvailability.UNAVAILABLE)
            }
    }
    fun <T : Any> exec(validator: SingleCallValidator<T>, onError: T): Mono<T> {
        return upstream.getIngressReader()
            .read(validator.method)
            .flatMap(ChainResponse::requireResult)
            .map { validator.check(it) }
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.warn("No response for ${validator.method.method} from ${upstream.getId()}") }
                    .then(Mono.error(TimeoutException("Validation timeout for ${validator.method.method}"))),
            )
            .doOnError { err -> log.error("Error during ${validator.method.method} validation for ${upstream.getId()}", err) }
            .onErrorReturn(onError)
    }

    override fun validateUpstreamSettings(): Mono<ValidateUpstreamSettingsResult> {
        return Mono.zip(
            startupValidators.map { exec(it, ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR) },
        ) { a -> a.map { it as ValidateUpstreamSettingsResult } }
            .map(::resolve)
            .defaultIfEmpty(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
            .onErrorResume {
                log.error("Error during upstream validation for ${upstream.getId()}", it)
                Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR)
            }
    }

    override fun validateUpstreamSettingsOnStartup(): ValidateUpstreamSettingsResult {
        return validateUpstreamSettings().block() ?: ValidateUpstreamSettingsResult.UPSTREAM_VALID
    }
}
