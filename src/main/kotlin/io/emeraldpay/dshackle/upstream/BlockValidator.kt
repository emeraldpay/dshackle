package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer

interface BlockValidator {

    fun isValid(currentHead: BlockContainer?, newHead: BlockContainer): Boolean

    class AlwaysValid : BlockValidator {
        override fun isValid(currentHead: BlockContainer?, newHead: BlockContainer): Boolean = true
    }

    companion object {
        @JvmField
        val ALWAYS_VALID = AlwaysValid()
    }
}
