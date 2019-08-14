package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.Closeable
import java.util.concurrent.atomic.AtomicReference

class EthereumHeadMerge(
        upstreams: List<EthereumHead>
): EthereumHead, Lifecycle {

    private val log = LoggerFactory.getLogger(EthereumHeadMerge::class.java)
    private val flux: Flux<BlockJson<TransactionId>>
    private val head = AtomicReference<BlockJson<TransactionId>>(null)
    private var subscription: Disposable? = null

    init {
        val fluxes = upstreams.map { it.getFlux() }
        flux = Flux.merge(fluxes)
                .distinctUntilChanged {
                    it.hash
                }
                .filter {
                    val curr = head.get()
                    curr == null || curr.totalDifficulty < it.totalDifficulty
                }
                .publish()
                .autoConnect()
    }

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        subscription = Flux.from(flux).subscribe {
            head.set(it)
        }
    }


    override fun getHead(): Mono<BlockJson<TransactionId>> {
        val curr = head.get()
        if (curr != null) {
            return Mono.just(curr)
        }
        return getFlux().next()
    }

    override fun getFlux(): Flux<BlockJson<TransactionId>> {
        return Flux.from(this.flux)
                .onBackpressureLatest()
    }

    override fun stop() {
        subscription?.dispose()
    }

}