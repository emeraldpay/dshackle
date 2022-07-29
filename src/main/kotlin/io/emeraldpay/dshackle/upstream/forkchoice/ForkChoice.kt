package io.emeraldpay.dshackle.upstream.forkchoice

import io.emeraldpay.dshackle.data.BlockContainer

interface ForkChoice {

    sealed class ChoiceResult {
        data class Updated(val nwhead: BlockContainer) : ChoiceResult()
        data class Same(val head: BlockContainer?) : ChoiceResult()
    }

    fun getHead(): BlockContainer?

    fun filter(block: BlockContainer): Boolean

    fun choose(block: BlockContainer): ChoiceResult
}
