package io.emeraldpay.dshackle.upstream.forkchoice

import io.emeraldpay.dshackle.data.BlockContainer
import java.util.concurrent.atomic.AtomicReference

class AlwaysForkChoice : ForkChoice {
    private val head = AtomicReference<BlockContainer>(null)

    override fun getHead(): BlockContainer? = head.get()

    override fun filter(block: BlockContainer): Boolean =
        head.get()?.let { it.hash != block.hash } ?: true

    override fun choose(block: BlockContainer): ForkChoice.ChoiceResult =
        head.updateAndGet { block }.let {
            if (it.hash == block.hash) {
                ForkChoice.ChoiceResult.Updated(it)
            } else {
                ForkChoice.ChoiceResult.Same(it)
            }
        }
}
