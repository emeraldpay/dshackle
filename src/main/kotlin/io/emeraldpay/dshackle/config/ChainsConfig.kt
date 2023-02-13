package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain

data class ChainsConfig(private val chains: Map<Chain, RawChainConfig>, val currentDefault: RawChainConfig?) {
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
        val raw = chains[chain] ?: default

        return ChainConfig(
            laggingLagSize = raw.laggingLagSize ?: default.laggingLagSize ?: panic(),
            syncingLagSize = raw.syncingLagSize ?: default.syncingLagSize ?: panic(),
        )
    }

    fun patch(patch: ChainsConfig) = ChainsConfig(
        merge(this.chains, patch.chains),
        merge(this.currentDefault!!, patch.currentDefault)
    )

    private fun merge(
        current: RawChainConfig,
        patch: RawChainConfig?
    ) = RawChainConfig(
        syncingLagSize = patch?.syncingLagSize ?: current.syncingLagSize,
        laggingLagSize = patch?.laggingLagSize ?: current.laggingLagSize
    )

    private fun merge(
        current: Map<Chain, RawChainConfig>,
        patch: Map<Chain, RawChainConfig>
    ): Map<Chain, RawChainConfig> {
        val currentMut = current.toMutableMap()

        for (k in patch) {
            currentMut.merge(k.key, k.value) { v1, v2 -> merge(v1, v2) }
        }

        return currentMut.toMap()
    }

    fun panic(): Nothing = throw IllegalStateException("Chains settings state is illegal - default config is null")
}
