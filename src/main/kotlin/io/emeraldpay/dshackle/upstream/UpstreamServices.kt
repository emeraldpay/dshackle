package io.emeraldpay.dshackle.upstream

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

class UpstreamServices {

    companion object {
        fun onceOk(up: Upstream, waitTime: Duration = Duration.ofSeconds(15)): Mono<Boolean> {
            return Flux.concat(Flux.just(up.getStatus()), up.observeStatus())
                    .timeout(waitTime, Mono.just(UpstreamAvailability.UNAVAILABLE))
                    .filter { it == UpstreamAvailability.OK }
                    .next()
                    .hasElement()
        }
    }
}