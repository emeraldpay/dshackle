package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.foundation.ChainOptions
import java.math.BigInteger
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

    class GasPriceCondition(private val condition: String) {
        fun check(value: Long): Boolean {
            val (op, valueStr) = condition.split(" ")
            return when (op) {
                "ne" -> value != valueStr.toLong()
                "eq" -> value == valueStr.toLong()
                "gt" -> value > valueStr.toLong()
                "lt" -> value < valueStr.toLong()
                "ge" -> value >= valueStr.toLong()
                "le" -> value <= valueStr.toLong()
                else -> throw IllegalArgumentException("Unsupported condition: $condition")
            }
        }

        fun rules() = condition
    }

    data class ChainConfig(
        val expectedBlockTime: Duration,
        val syncingLagSize: Int,
        val laggingLagSize: Int,
        val options: ChainOptions.PartialOptions,
        val chainId: String,
        val netVersion: BigInteger,
        val grpcId: Int,
        val code: String,
        val shortNames: List<String>,
        val callLimitContract: String?,
        val id: String,
        val blockchain: String,
        val type: String,
        val gasPriceCondition: GasPriceCondition? = null,
    ) {
        companion object {
            @JvmStatic
            fun default() = defaultWithContract(null)

            @JvmStatic
            fun defaultWithContract(callLimitContract: String?) = ChainConfig(
                Duration.ofSeconds(12),
                6,
                1,
                ChainOptions.PartialOptions(),
                "0x0",
                BigInteger.ZERO,
                0,
                "UNKNOWN",
                emptyList(),
                callLimitContract,
                "undefined",
                "undefined",
                "unknown",
                null,
            )

            @JvmStatic
            fun defaultWithGasPriceCondition(gasPriceCondition: String) = defaultWithContract(null).copy(
                gasPriceCondition = GasPriceCondition(gasPriceCondition),
            )
        }
    }

    fun resolve(chain: String): ChainConfig {
        return chainMap[chain] ?: ChainConfig.default()
    }
}
