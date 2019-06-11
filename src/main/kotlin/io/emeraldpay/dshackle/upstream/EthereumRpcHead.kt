package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class EthereumRpcHead(
    private val api: EthereumApi
): EthereumHead {

    private val log = LoggerFactory.getLogger(EthereumRpcHead::class.java)

    private val head = AtomicReference<BlockJson<TransactionId>>(null)
    private val stream: TopicProcessor<BlockJson<TransactionId>> = TopicProcessor.create()

    fun start() {
        Flux.interval(Duration.ofSeconds(7))
                .flatMap {
                    val batch = Batch()
                    val f = batch.add(Commands.eth().blockNumber)
                    api.execute(batch)
                    Mono.fromCompletionStage(f).timeout(Duration.ofSeconds(5))
                }
                .flatMap {
                    val batch = Batch()
                    val f = batch.add(Commands.eth().getBlock(it))
                    api.execute(batch)
                    Mono.fromCompletionStage(f).timeout(Duration.ofSeconds(5))
                }
                .onErrorContinue { err, _ ->
                    log.warn("RPC error ${err.message}")
                }
                .filter { block ->
                    val curr = head.get()
                    curr == null || curr.totalDifficulty < block.totalDifficulty
                }
                .subscribe { block ->
                    stream.onNext(block)
                }

        Flux.from(this.stream)
                .subscribe { head.set(it) }
    }

    override fun getHead(): Mono<BlockJson<TransactionId>> {
        val current = head.get()
        if (current != null) {
            return Mono.just(current)
        }
        return Mono.from(stream)
    }

    override fun getFlux(): Flux<BlockJson<TransactionId>> {
        return Flux.from(stream)
    }

}