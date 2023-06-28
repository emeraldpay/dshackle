package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class ChainsConfigTest {

    @Test
    fun patch() {
        val orig = ChainsConfig(
            mapOf(
                Chain.BITCOIN__MAINNET to createRawChainConfig(0, 0),
                Chain.ETHEREUM__MAINNET to createRawChainConfig(1, 2),
                Chain.POLYGON_POS__MAINNET to createRawChainConfig(3, 4)
            ),
            createRawChainConfig(1, 2)
        )

        val patch = ChainsConfig(
            mapOf(
                Chain.BITCOIN__MAINNET to createRawChainConfig(null, 10000),
                Chain.POLYGON_POS__MAINNET to createRawChainConfig(10, 11),
                Chain.ARBITRUM__MAINNET to createRawChainConfig(999, 999)
            ),
            createRawChainConfig(100, null)
        )

        val res = orig.patch(patch)

        assertEquals(
            ChainsConfig(
                mapOf(
                    Chain.BITCOIN__MAINNET to createRawChainConfig(0, 10000),
                    Chain.ETHEREUM__MAINNET to createRawChainConfig(1, 2),
                    Chain.POLYGON_POS__MAINNET to createRawChainConfig(10, 11),
                    Chain.ARBITRUM__MAINNET to createRawChainConfig(999, 999)
                ),
                createRawChainConfig(100, 2)
            ),
            res
        )
    }

    private fun createRawChainConfig(syncingLagSize: Int?, laggingLagSize: Int?) =
        ChainsConfig.RawChainConfig()
            .apply {
                this.syncingLagSize = syncingLagSize
                this.laggingLagSize = laggingLagSize
            }
}
