package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer

interface BlockValidator {

    fun isValid(block: BlockContainer): Boolean

    class AlwaysValid : BlockValidator {
        override fun isValid(block: BlockContainer): Boolean = true
    }

    companion object {
        val ALWAYS_VALID = AlwaysValid()
    }
}