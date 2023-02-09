package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain

class ChainsConfig(var chains: Map<Chain, ChainConfig>, val currentDefault: ChainConfig) {
    companion object {
        @JvmStatic
        fun default(): ChainsConfig = ChainsConfig(emptyMap(), ChainConfig.default())
    }

    data class ChainConfig(val syncingLagSize: Int, val laggingLagSize: Int) {
        companion object {
            @JvmStatic
            fun default() = ChainConfig(6, 1)
        }
    }

    fun resolve(chain: Chain) = chains[chain] ?: currentDefault
}
