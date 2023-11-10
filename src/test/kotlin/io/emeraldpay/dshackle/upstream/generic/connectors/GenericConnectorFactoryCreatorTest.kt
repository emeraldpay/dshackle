package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.ChainsConfigReader
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptionsReader
import io.emeraldpay.dshackle.startup.configure.GenericConnectorFactoryCreator
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.forkchoice.AlwaysForkChoice
import io.emeraldpay.dshackle.upstream.generic.GenericRpcHead
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito.mockConstruction
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.core.scheduler.Schedulers.immediate
import java.io.File
import java.net.URI
import java.time.Duration

class GenericConnectorFactoryCreatorTest {

    @ParameterizedTest
    @MethodSource("data")
    fun `rpc head poll interval must equal to expected-block-time`(
        expectedBlockTime: Duration,
        config: ChainsConfig.ChainConfig,
    ) {
        val factory = GenericConnectorFactoryCreator(
            FileResolver(File("")),
            immediate(),
            immediate(),
            immediate(),
            immediate(),
        )
        var args: List<*>? = null

        mockConstruction(GenericRpcHead::class.java) { _, ctx -> args = ctx.arguments() }
            .use {
                factory.createConnectorFactoryCreator(
                    "id",
                    UpstreamsConfig.RpcConnection(
                        UpstreamsConfig.HttpEndpoint(URI("http://localhost")),
                    ),
                    Chain.ETHEREUM__MAINNET,
                    AlwaysForkChoice(),
                    BlockValidator.ALWAYS_VALID,
                    config,
                )?.create(mock<DefaultUpstream> { on { getId() } doReturn "id" }, Chain.ETHEREUM__MAINNET)

                assertEquals(expectedBlockTime, args?.get(6))
            }
    }

    companion object {
        private val chainsConfigReader = ChainsConfigReader(ChainOptionsReader())

        @JvmStatic
        fun data(): List<Arguments> = chainsConfigReader.read(null)
            .flatMap { cfg ->
                cfg.shortNames.map {
                    Arguments.of(cfg.expectedBlockTime.coerceAtLeast(Duration.ofSeconds(1)), cfg)
                }
            }
    }
}
