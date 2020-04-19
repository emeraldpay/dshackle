package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import java.util.concurrent.atomic.AtomicReference

abstract class AbstractHead : Head {

    companion object {
        private val log = LoggerFactory.getLogger(AbstractHead::class.java)
    }

    private val head = AtomicReference<BlockContainer>(null)
    private val stream: TopicProcessor<BlockContainer> = TopicProcessor.create()

    fun follow(source: Flux<BlockContainer>): Disposable {
        return source.distinctUntilChanged {
            it.hash
        }.filter { block ->
            val curr = head.get()
            curr == null || curr.difficulty < block.difficulty
        }
                .subscribe { block ->
                    val prev = head.getAndUpdate { curr ->
                        if (curr == null || curr.difficulty < block.difficulty) {
                            block
                        } else {
                            curr
                        }
                    }
                    if (prev == null || prev.hash != block.hash) {
                        log.debug("New block ${block.height} ${block.hash}")
                        stream.onNext(block)
                    }
                }
    }

    override fun getFlux(): Flux<BlockContainer> {
        return Flux.merge(
                Mono.justOrEmpty(head.get()),
                Flux.from(stream)
        ).onBackpressureLatest()
    }

    fun getCurrent(): BlockContainer? {
        return head.get()
    }
}