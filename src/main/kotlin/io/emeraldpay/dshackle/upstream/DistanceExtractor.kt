package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer

class DistanceExtractor {
    sealed class ChainDistance {
        data class Distance(val dist: Long) : ChainDistance()
        object Fork : ChainDistance()
    }

    companion object {
        fun extractPowDistance(top: BlockContainer, curr: BlockContainer): ChainDistance {
            return when {
                curr.height > top.height -> if (curr.difficulty >= top.difficulty) ChainDistance.Distance(0) else ChainDistance.Fork
                curr.height == top.height -> if (curr.difficulty == top.difficulty) ChainDistance.Distance(0) else ChainDistance.Fork
                else -> ChainDistance.Distance(top.height - curr.height)
            }
        }

        fun extractPriorityDistance(top: BlockContainer, curr: BlockContainer): ChainDistance {
            return when {
                (curr.parentHash != null && curr.height - top.height == 1L) ->
                    if (curr.parentHash == top.hash) ChainDistance.Distance(0) else ChainDistance.Fork
                curr.height == top.height -> if (curr.hash == top.hash) ChainDistance.Distance(0) else ChainDistance.Fork
                else -> ChainDistance.Distance((top.height - curr.height).coerceAtLeast(0))
            }
        }
    }
}
