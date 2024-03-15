package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.foundation.ChainOptions
import reactor.core.publisher.Mono
import java.util.concurrent.TimeoutException
import java.util.function.Supplier

abstract class BasicEthUpstreamValidator(
    upstream: Upstream,
    options: ChainOptions.Options,
) : UpstreamValidator(upstream, options) {

    override fun validate(): Mono<UpstreamAvailability> {
        return Mono.zip(
            validatorFunctions().map { it.get() },
        ) { a -> a.map { it as UpstreamAvailability } }
            .map(::resolve)
            .defaultIfEmpty(UpstreamAvailability.UNAVAILABLE)
            .onErrorResume {
                log.error("Error during upstream validation for ${upstream.getId()}", it)
                Mono.just(UpstreamAvailability.UNAVAILABLE)
            }
    }

    protected fun validateSyncing(): Mono<UpstreamAvailability> {
        if (!options.validateSyncing) {
            return Mono.just(UpstreamAvailability.OK)
        }
        val validateSyncingRequest = validateSyncingRequest()
        return upstream.getIngressReader()
            .read(validateSyncingRequest.request)
            .flatMap(ChainResponse::requireResult)
            .map { validateSyncingRequest.mapper(it) }
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.warn("No response for ${validateSyncingRequest.request.method} from ${upstream.getId()}") }
                    .then(Mono.error(TimeoutException("Validation timeout for Syncing"))),
            )
            .map {
                upstream.getHead().onSyncingNode(it)
                if (it) {
                    UpstreamAvailability.SYNCING
                } else {
                    UpstreamAvailability.OK
                }
            }
            .doOnError { err -> log.error("Error during syncing validation for ${upstream.getId()}", err) }
            .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    protected fun validatePeers(): Mono<UpstreamAvailability> {
        if (!options.validatePeers || options.minPeers == 0) {
            return Mono.just(UpstreamAvailability.OK)
        }
        val validatePeersRequest = validatePeersRequest()
        return upstream
            .getIngressReader()
            .read(validatePeersRequest.request)
            .flatMap(ChainResponse::checkError)
            .map { validatePeersRequest.mapper(it) }
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.warn("No response for ${validatePeersRequest.request.method} from ${upstream.getId()}") }
                    .then(Mono.error(TimeoutException("Validation timeout for Peers"))),
            )
            .map { count ->
                val minPeers = options.minPeers
                if (count < minPeers) {
                    UpstreamAvailability.IMMATURE
                } else {
                    UpstreamAvailability.OK
                }
            }
            .doOnError { err -> log.error("Error during peer count validation for ${upstream.getId()}", err) }
            .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    protected abstract fun validateSyncingRequest(): ValidateSyncingRequest

    protected abstract fun validatePeersRequest(): ValidatePeersRequest

    protected abstract fun validatorFunctions(): List<Supplier<Mono<UpstreamAvailability>>>

    data class ValidateSyncingRequest(
        val request: ChainRequest,
        val mapper: (ByteArray) -> Boolean,
    )

    data class ValidatePeersRequest(
        val request: ChainRequest,
        val mapper: (ChainResponse) -> Int,
    )
}
