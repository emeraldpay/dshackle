package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.config.hot.CompatibleVersionsRules
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.UpstreamAvailability.OK
import io.emeraldpay.dshackle.upstream.UpstreamAvailability.UNAVAILABLE
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException
import java.util.function.Supplier

typealias UpstreamValidatorBuilder = (Chain, Upstream, ChainOptions.Options, ChainConfig, Supplier<CompatibleVersionsRules?>) -> UpstreamValidator?

abstract class UpstreamValidator(
    val upstream: Upstream,
    val options: ChainOptions.Options,
) {
    fun start(): Flux<UpstreamAvailability> {
        return Flux.interval(
            Duration.ZERO,
            Duration.ofSeconds(options.validationInterval.toLong()),
        ).subscribeOn(scheduler)
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

        @JvmStatic
        protected val log: Logger = LoggerFactory.getLogger(UpstreamValidator::class.java)

        val scheduler =
            Schedulers.fromExecutor(Executors.newFixedThreadPool(4, CustomizableThreadFactory("upstream-validator")))
    }
}

enum class ValidateUpstreamSettingsResult(val priority: Int) {
    UPSTREAM_VALID(0),
    UPSTREAM_SETTINGS_ERROR(1),
    UPSTREAM_FATAL_SETTINGS_ERROR(2),
}

interface SingleValidator<T> {
    fun validate(onError: T): Mono<T>
}

class GenericSingleCallValidator<T : Any>(
    val method: ChainRequest,
    val upstream: Upstream,
    val check: (ByteArray) -> T,
) : SingleValidator<T> {

    companion object {
        @JvmStatic
        val log: Logger = LoggerFactory.getLogger(GenericSingleCallValidator::class.java)
    }
    override fun validate(onError: T): Mono<T> {
        return upstream.getIngressReader()
            .read(method)
            .flatMap(ChainResponse::requireResult)
            .map { check(it) }
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.warn("No response for ${method.method} from ${upstream.getId()}") }
                    .then(Mono.error(TimeoutException("Validation timeout for ${method.method}"))),
            )
            .doOnError { err -> log.error("Error during ${method.method} validation for ${upstream.getId()}", err) }
            .onErrorReturn(onError)
    }
}

class VersionValidator(
    private val upstream: Upstream,
    private val versionsConfig: Supplier<CompatibleVersionsRules?>,
) : SingleValidator<UpstreamAvailability> {
    companion object {
        private val log = LoggerFactory.getLogger(VersionValidator::class.java)
    }
    override fun validate(onError: UpstreamAvailability): Mono<UpstreamAvailability> {
        if (upstream.getUpstreamSettingsData() == null) {
            // for now - just skip validation
            log.info("Empty settings for upstream ${upstream.getId()}, skipping version validation")
            return Mono.just(OK)
        }
        val type = upstream.getLabels().first().getOrDefault("client_type", "unknown")
        val version = upstream.getLabels().first().getOrDefault("client_version", "unknown")
        val rule = versionsConfig.get()!!.rules.find { it.client == type }
        if (rule == null) {
            log.info("No rules for client type $type, skipping validation for upstream ${upstream.getId()}")
            return Mono.just(OK)
        }
        if (!rule.whitelist.isNullOrEmpty() && !rule.whitelist.contains(version)) {
            log.warn("Version $version is in not in defined whitelist for $type, please change client version for upstream ${upstream.getId()}")
            return Mono.just(UNAVAILABLE)
        }
        if (!rule.blacklist.isNullOrEmpty() && rule.blacklist.contains(version)) {
            log.warn("Version $version is in defined blacklist for $type, please change client version for upstream ${upstream.getId()}")
            return Mono.just(UNAVAILABLE)
        }
        return Mono.just(OK)
    }
}
