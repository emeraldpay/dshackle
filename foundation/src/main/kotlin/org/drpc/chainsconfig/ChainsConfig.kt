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

    class GasPriceCondition(rawConditions: List<String>) {
        private val conditions: List<Pair<String, Long>> = rawConditions.map {
            val parts = it.split(" ")
            if (parts.size != 2 || listOf("ne", "eq", "gt", "lt", "ge", "le").none { op -> op == parts[0] }) {
                throw IllegalArgumentException("Invalid condition: $it")
            }
            Pair(parts[0], parts[1].toLong())
        }

        fun check(value: Long): Boolean {
            return conditions.all { (op, limit) ->
                when (op) {
                    "ne" -> value != limit
                    "eq" -> value == limit
                    "gt" -> value > limit
                    "lt" -> value < limit
                    "ge" -> value >= limit
                    "le" -> value <= limit
                    else -> false
                }
            }
        }

        fun rules() = conditions.joinToString { (op, limit) -> "$op $limit" }
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
        val gasPriceCondition: GasPriceCondition,
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
                GasPriceCondition(emptyList()),
            )

            @JvmStatic
            fun defaultWithGasPriceCondition(gasPriceConditions: List<String>) = defaultWithContract(null).copy(
                gasPriceCondition = GasPriceCondition(gasPriceConditions),
            )
        }
    }

    fun resolve(chain: String): ChainConfig {
        return chainMap[chain] ?: ChainConfig.default()
    }
}
