package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.infinitape.etherjar.rpc.Commands
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.Executors

class BitcoinUpstreamValidator(
        private val api: BitcoinApi,
        private val options: UpstreamsConfig.Options
) {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinUpstreamValidator::class.java)
        val scheduler = Schedulers.fromExecutor(Executors.newCachedThreadPool(CustomizableThreadFactory("bitcoin-validator")))
    }

    fun validate(): Mono<UpstreamAvailability> {
        return api.executeAndResult(0, "getconnectioncount", emptyList(), Int::class.java)
                .map { count ->
                    val minPeers = options.minPeers ?: 1
                    if (count < minPeers) {
                        UpstreamAvailability.IMMATURE
                    } else {
                        UpstreamAvailability.OK
                    }
                }
                .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    fun start(): Flux<UpstreamAvailability> {
        return Flux.interval(Duration.ofSeconds(15))
                .subscribeOn(scheduler)
                .flatMap {
                    validate()
                }
    }

}