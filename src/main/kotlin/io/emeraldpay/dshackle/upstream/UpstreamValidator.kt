package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

typealias UpstreamValidatorBuilder = (Chain, Upstream, ChainOptions.Options, ChainConfig) -> UpstreamValidator?

abstract class UpstreamValidator(
    val upstream: Upstream,
    val options: ChainOptions.Options,
) {
    protected val log = LoggerFactory.getLogger(this::class.java)

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

    open fun validateUpstreamSettingsOnStartup(): ValidateUpstreamSettingsResult {
        return ValidateUpstreamSettingsResult.UPSTREAM_VALID
    }

    companion object {
        @JvmStatic
        fun resolve(results: Iterable<UpstreamAvailability>): UpstreamAvailability {
            val cp = Comparator { avail1: UpstreamAvailability, avail2: UpstreamAvailability -> if (avail1.isBetterTo(avail2)) -1 else 1 }
            return results.sortedWith(cp).last()
        }

        fun resolve(results: Iterable<ValidateUpstreamSettingsResult>): ValidateUpstreamSettingsResult {
            val cp = Comparator { res1: ValidateUpstreamSettingsResult, res2: ValidateUpstreamSettingsResult -> if (res1.priority < res2.priority) -1 else 1 }
            return results.sortedWith(cp).last()
        }
    }
}

enum class ValidateUpstreamSettingsResult(val priority: Int) {
    UPSTREAM_VALID(0),
    UPSTREAM_SETTINGS_ERROR(1),
    UPSTREAM_FATAL_SETTINGS_ERROR(2),
}

data class SingleCallValidator<T>(
    val method: ChainRequest,
    val check: (ByteArray) -> T,
)
