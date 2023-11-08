package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.config.UpstreamsConfig
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito.reset
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify

class UpstreamFactoryTest {

    @BeforeEach
    fun beforeEach() {
        reset(genericUpstreamCreator, ethereumUpstreamCreator, bitcoinUpstreamCreator)
    }

    @ParameterizedTest
    @MethodSource("data")
    fun `create upstream based on chain`(
        blockchain: BlockchainType,
        verify: Runnable,
    ) {
        upstreamFactory.createUpstream(blockchain, UpstreamsConfig.Upstream<UpstreamsConfig.UpstreamConnection>(), emptyMap())

        verify.run()
    }

    companion object {
        private val genericUpstreamCreator = mock<GenericUpstreamCreator>()
        private val ethereumUpstreamCreator = mock<EthereumUpstreamCreator>()
        private val bitcoinUpstreamCreator = mock<BitcoinUpstreamCreator>()
        private val upstreamFactory = UpstreamFactory(genericUpstreamCreator, ethereumUpstreamCreator, bitcoinUpstreamCreator)

        @JvmStatic
        fun data() = listOf(
            Arguments.of(
                BlockchainType.ETHEREUM,
                Runnable { verify(ethereumUpstreamCreator).createUpstream(any(), any()) },
            ),
            Arguments.of(
                BlockchainType.BITCOIN,
                Runnable { verify(bitcoinUpstreamCreator).createUpstream(any(), any()) },
            ),
            Arguments.of(
                BlockchainType.POLKADOT,
                Runnable { verify(genericUpstreamCreator).createUpstream(any(), any()) },
            ),
            Arguments.of(
                BlockchainType.STARKNET,
                Runnable { verify(genericUpstreamCreator).createUpstream(any(), any()) },
            ),
            Arguments.of(
                BlockchainType.UNKNOWN,
                Runnable { verify(genericUpstreamCreator).createUpstream(any(), any()) },
            ),
        )
    }
}
