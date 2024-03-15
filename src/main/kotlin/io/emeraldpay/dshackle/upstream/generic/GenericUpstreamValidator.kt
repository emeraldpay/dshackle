package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.SingleCallValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult.UPSTREAM_VALID
import reactor.core.publisher.Mono
import java.util.concurrent.TimeoutException

class GenericUpstreamValidator(
    upstream: Upstream,
    options: ChainOptions.Options,
    private val validator: SingleCallValidator,
) : UpstreamValidator(upstream, options) {

    override fun validate(): Mono<UpstreamAvailability> {
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
            .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    override fun validateUpstreamSettings(): Mono<ValidateUpstreamSettingsResult> {
        return Mono.just(UPSTREAM_VALID)
    }
}
