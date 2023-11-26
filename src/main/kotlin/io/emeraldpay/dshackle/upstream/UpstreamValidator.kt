package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

typealias UpstreamValidatorBuilder = (Chain, Upstream, ChainOptions.Options, ChainConfig) -> UpstreamValidator?

abstract class UpstreamValidator(
    val upstream: Upstream,
    val options: ChainOptions.Options,
) {
    companion object {
        private val log = LoggerFactory.getLogger(UpstreamValidator::class.java)
    }
    fun start(): Flux<UpstreamAvailability> {
        return Flux.interval(
            Duration.ZERO,
            Duration.ofSeconds(options.validationInterval.toLong()),
        ).subscribeOn(EthereumUpstreamValidator.scheduler)
            .flatMap {
                validate()
            }
            .doOnNext {
                log.debug("Status after validation is {} for {}", it, upstream.getId())
            }
    }

    abstract fun validate(): Mono<UpstreamAvailability>

    abstract fun validateUpstreamSettings(): Mono<ValidateUpstreamSettingsResult>

    fun validateUpstreamSettingsOnStartup(): ValidateUpstreamSettingsResult {
        return validateUpstreamSettings().block() ?: ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
    }
}

enum class ValidateUpstreamSettingsResult {
    UPSTREAM_VALID,
    UPSTREAM_SETTINGS_ERROR,
    UPSTREAM_FATAL_SETTINGS_ERROR,
}

data class SingleCallValidator(
    val method: JsonRpcRequest,
    val check: (ByteArray) -> UpstreamAvailability,
)
