package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.Head
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class FixedHead(
    var height: Long? = null,
    var block: BlockContainer? = null,
) : Head {
    companion object {
        private val log = LoggerFactory.getLogger(FixedHead::class.java)
    }

    override fun getFlux(): Flux<BlockContainer> = Flux.from(Mono.justOrEmpty(block))

    override fun onBeforeBlock(handler: Runnable) {
    }

    override fun getCurrentHeight(): Long? = height ?: block?.height
}
