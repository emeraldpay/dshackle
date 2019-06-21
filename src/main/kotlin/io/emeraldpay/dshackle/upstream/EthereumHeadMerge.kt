package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.util.concurrent.atomic.AtomicReference

class EthereumHeadMerge(
        private val upstreams: List<EthereumHead>
): EthereumHead {

    private val log = LoggerFactory.getLogger(EthereumHeadMerge::class.java)
    private val flux: Flux<BlockJson<TransactionId>>
    private val head = AtomicReference<BlockJson<TransactionId>>(null)

    init {
        val fluxes = upstreams.map { it.getFlux() }
        flux = Flux.merge(fluxes)
                .filter {
                    val curr = head.get()
                    curr == null || curr.totalDifficulty < it.totalDifficulty
                }
                .publish()
                .autoConnect()

        Flux.from(flux).subscribe {
            head.set(it)
        }
    }

    override fun getHead(): Mono<BlockJson<TransactionId>> {
        val curr = head.get()
        if (curr != null) {
            return Mono.just(curr)
        }
        return Mono.from(getFlux())
    }

    override fun getFlux(): Flux<BlockJson<TransactionId>> {
        return Flux.from(this.flux)
                .onBackpressureLatest()
    }
}