package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain
import java.lang.IllegalStateException

class ChainsConfig(private val chains: Map<Chain, RawChainConfig>?, val currentDefault: RawChainConfig?) {
    companion object {
        @JvmStatic
        fun default(): ChainsConfig = ChainsConfig(emptyMap(), RawChainConfig.default())
    }

    data class RawChainConfig(val syncingLagSize: Int?, val laggingLagSize: Int?) {
        companion object {
            @JvmStatic
            fun default() = RawChainConfig(6, 1)
        }
    }

    data class ChainConfig(val syncingLagSize: Int, val laggingLagSize: Int) {
        companion object {
            @JvmStatic
            fun default() = ChainConfig(6, 1)
        }
    }

    fun resolve(chain: Chain): ChainConfig {
        val default = currentDefault ?: panic()
        val raw = chains?.get(chain) ?: default

        return ChainConfig(
            laggingLagSize = raw.laggingLagSize ?: default.laggingLagSize ?: panic(),
            syncingLagSize = raw.syncingLagSize ?: default.syncingLagSize ?: panic(),
        )
    }

    fun panic(): Nothing = throw IllegalStateException("Chains settings state is illegal - default config is null")
}
