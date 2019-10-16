package io.emeraldpay.dshackle.upstream.ethereum

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import java.util.concurrent.atomic.AtomicReference

open class DefaultEthereumHead: EthereumHead {

    private val log = LoggerFactory.getLogger(DefaultEthereumHead::class.java)
    private val head = AtomicReference<BlockJson<TransactionRefJson>>(null)
    private val stream: TopicProcessor<BlockJson<TransactionRefJson>> = TopicProcessor.create()

    fun follow(source: Flux<BlockJson<TransactionRefJson>>): Disposable {
        return source.distinctUntilChanged {
            it.hash
        }.filter { block ->
            val curr = head.get()
            curr == null || curr.totalDifficulty < block.totalDifficulty
        }
        .subscribe { block ->
            val prev = head.getAndUpdate { curr ->
                if (curr == null || curr.totalDifficulty < block.totalDifficulty) {
                    block
                } else {
                    curr
                }
            }
            if (prev == null || prev.hash != block.hash) {
                log.debug("New block ${block.number} ${block.hash}")
                stream.onNext(block)
            }
        }
    }

    override fun getFlux(): Flux<BlockJson<TransactionRefJson>> {
        return Flux.merge(
                Mono.justOrEmpty(head.get()),
                Flux.from(stream)
        ).onBackpressureLatest()
    }

    fun getCurrent(): BlockJson<TransactionRefJson>? {
        return head.get()
    }
}