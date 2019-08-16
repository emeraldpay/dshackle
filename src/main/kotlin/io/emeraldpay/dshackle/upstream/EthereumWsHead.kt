package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference

class EthereumWsHead(
        private val ws: EthereumWs
): EthereumHead, Lifecycle {

    private val log = LoggerFactory.getLogger(EthereumWsHead::class.java)

    private var subscription: Disposable? = null
    private val head = AtomicReference<BlockJson<TransactionId>>(null)
    private var stream: Flux<BlockJson<TransactionId>>? = null

    override fun getHead(): Mono<BlockJson<TransactionId>> {
        val current = head.get()
        if (current != null) {
            return Mono.just(current)
        }
        return Mono.from(getFlux())
    }

    override fun getFlux(): Flux<BlockJson<TransactionId>> {
        return stream?.let { Flux.from(it) } ?: Flux.error(Exception("Not started"))
    }

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        val flux = ws.getFlux()
            .distinctUntilChanged { it.hash }
            .filter { block ->
                val curr = head.get()
                curr == null || curr.totalDifficulty < block.totalDifficulty
            }.share()

        this.subscription = flux.subscribe(head::set)
        this.stream = flux
    }

    override fun stop() {
        subscription?.dispose()
        subscription = null
    }

}