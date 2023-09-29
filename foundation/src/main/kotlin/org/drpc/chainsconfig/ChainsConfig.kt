package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.foundation.ChainOptions
import java.time.Duration

data class ChainsConfig(private val chains: List<ChainConfig>) : Iterable<ChainsConfig.ChainConfig> {
    private val chainMap: Map<String, ChainConfig> = chains.fold(emptyMap()) { acc, item ->
        acc.plus(item.shortNames.map { Pair(it, item) })
    }

    override fun iterator(): Iterator<ChainConfig> {
        return chains.iterator()
    }
    companion object {
        @JvmStatic
        fun default(): ChainsConfig = ChainsConfig(emptyList())
    }

    data class ChainConfig(
        val expectedBlockTime: Duration,
        val syncingLagSize: Int,
        val laggingLagSize: Int,
        val options: ChainOptions.PartialOptions,
        val chainId: String,
        val netVersion: Long,
        val grpcId: Int,
        val code: String,
        val shortNames: List<String>,
        val callLimitContract: String?,
        val id: String,
        val blockchain: String,
    ) {
        companion object {
            @JvmStatic
            fun default() = ChainConfig(
                Duration.ofSeconds(12),
                6,
                1,
                ChainOptions.PartialOptions(),
                "0x0",
                0,
                0,
                "UNKNOWN",
                emptyList(),
                null,
                "undefined",
                "undefined",
            )
        }
    }

    fun resolve(chain: String): ChainConfig {
        return chainMap[chain] ?: ChainConfig.default()
    }
}
