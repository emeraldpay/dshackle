package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class EthereumRpcHead(
    private val api: DirectEthereumApi
): EthereumHead, Lifecycle {

    private val log = LoggerFactory.getLogger(EthereumRpcHead::class.java)

    private val head = AtomicReference<BlockJson<TransactionId>>(null)
    private val stream: TopicProcessor<BlockJson<TransactionId>> = TopicProcessor.create()
    private var refreshSubscription: Disposable? = null

    override fun start() {
        refreshSubscription = Flux.interval(Duration.ofSeconds(7))
                .flatMap {
                    val batch = Batch()
                    val f = batch.add(Commands.eth().blockNumber)
                    api.rpcClient.execute(batch)
                    Mono.fromCompletionStage(f).timeout(Duration.ofSeconds(5))
                }
                .flatMap {
                    val batch = Batch()
                    val f = batch.add(Commands.eth().getBlock(it))
                    api.rpcClient.execute(batch)
                    Mono.fromCompletionStage(f).timeout(Duration.ofSeconds(5))
                }
                .onErrorContinue { err, _ ->
                    log.warn("RPC error ${err.message}")
                }
                .distinctUntilChanged { it.hash }
                .filter { block ->
                    val curr = head.get()
                    curr == null || curr.totalDifficulty < block.totalDifficulty
                }
                .subscribe { block ->
                    head.set(block)
                    stream.onNext(block)
                }
    }

    override fun isRunning(): Boolean {
        return refreshSubscription != null
    }

    override fun stop() {
        refreshSubscription?.dispose()
        refreshSubscription = null
    }

    override fun getFlux(): Flux<BlockJson<TransactionId>> {
        return Flux.merge(
                Mono.justOrEmpty(head.get()),
                Flux.from(stream)
        ).onBackpressureLatest()
    }

}