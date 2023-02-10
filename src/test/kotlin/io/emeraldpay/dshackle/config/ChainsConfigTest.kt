package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class ChainsConfigTest {

    @Test
    fun patch() {
        val orig = ChainsConfig(
            mapOf(
                Chain.BITCOIN to ChainsConfig.RawChainConfig(0, 0),
                Chain.ETHEREUM to ChainsConfig.RawChainConfig(1, 2),
                Chain.POLYGON to ChainsConfig.RawChainConfig(3, 4)
            ),
            ChainsConfig.RawChainConfig(1, 2)
        )

        val patch = ChainsConfig(
            mapOf(
                Chain.BITCOIN to ChainsConfig.RawChainConfig(null, 10000),
                Chain.POLYGON to ChainsConfig.RawChainConfig(10, 11),
                Chain.ARBITRUM to ChainsConfig.RawChainConfig(999, 999)
            ),
            ChainsConfig.RawChainConfig(100, null)
        )

        val res = orig.patch(patch)

        assertEquals(
            ChainsConfig(
                mapOf(
                    Chain.BITCOIN to ChainsConfig.RawChainConfig(0, 10000),
                    Chain.ETHEREUM to ChainsConfig.RawChainConfig(1, 2),
                    Chain.POLYGON to ChainsConfig.RawChainConfig(10, 11),
                    Chain.ARBITRUM to ChainsConfig.RawChainConfig(999, 999)
                ),
                ChainsConfig.RawChainConfig(100, 2)
            ),
            res
        )
    }
}
