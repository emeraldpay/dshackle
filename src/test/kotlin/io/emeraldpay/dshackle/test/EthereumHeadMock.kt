package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.Head
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks

class EthereumHeadMock : Head {
    private val bus = Sinks.many().multicast().onBackpressureBuffer<BlockContainer>()
    var predefined: Publisher<BlockContainer>? = null
        set(value) {
            field =
                Flux
                    .from(value)
                    .publish()
                    .refCount(1)
                    // keep the current block as latest, because getFlux is also used to get the current height
                    .doOnNext { latest = it }
        }

    private var latest: BlockContainer? = null
    private val handlers = mutableListOf<Runnable>()

    fun nextBlock(block: BlockContainer) {
        handlers.forEach {
            it.run()
        }
        println("New block: ${block.height} / ${block.hash}")
        latest = block
        bus.tryEmitNext(block)
    }

    override fun getFlux(): Flux<BlockContainer> =
        if (predefined != null) {
            Flux.concat(Mono.justOrEmpty(latest), Flux.from(predefined))
        } else {
            Flux.concat(Mono.justOrEmpty(latest).cast(BlockContainer::class.java), bus.asFlux()).distinctUntilChanged()
        }

    override fun onBeforeBlock(handler: Runnable) {
        handlers.add(handler)
    }

    override fun getCurrentHeight(): Long? = latest?.height
}
