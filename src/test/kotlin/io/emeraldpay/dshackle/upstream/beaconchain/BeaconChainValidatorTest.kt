package io.emeraldpay.dshackle.upstream.beaconchain

import io.emeraldpay.dshackle.Chain.ETH_BEACON_CHAIN__MAINNET
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.core.publisher.Mono

class BeaconChainValidatorTest {

    @ParameterizedTest
    @MethodSource("data")
    fun `validation beacon chain`(
        respHealth: Mono<ChainResponse>,
        respSync: Mono<ChainResponse>,
        respPeers: Mono<ChainResponse>,
        expected: UpstreamAvailability,
    ) {
        val reader = mock<ChainReader> {
            on { read(ChainRequest("GET#/eth/v1/node/health", RestParams.emptyParams())) } doReturn
                respHealth
            on { read(ChainRequest("GET#/eth/v1/node/syncing", RestParams.emptyParams())) } doReturn
                respSync
            on { read(ChainRequest("GET#/eth/v1/node/peer_count", RestParams.emptyParams())) } doReturn
                respPeers
        }
        val upstream = mock<Upstream> {
            on { getIngressReader() } doReturn reader
            on { getHead() } doReturn mock<Head>()
        }
        val validator = BeaconChainSpecific.validator(
            ETH_BEACON_CHAIN__MAINNET,
            upstream,
            ChainOptions.PartialOptions.getDefaults().buildOptions(),
            ChainConfig.default(),
            { null },
        )

        val result = validator.validate().block()

        assertEquals(expected, result)
    }

    companion object {
        @JvmStatic
        fun data(): List<Arguments> {
            return listOf(
                Arguments.of(
                    Mono.just(ChainResponse(ByteArray(0), null)),
                    Mono.just(ChainResponse("""{"data": {"is_syncing": false}}""".toByteArray(), null)),
                    Mono.just(ChainResponse("""{"data": {"connected": "20"}}""".toByteArray(), null)),
                    UpstreamAvailability.OK,
                ),
                Arguments.of(
                    Mono.error<ChainResponse>(RuntimeException("error")),
                    Mono.just(ChainResponse("""{"data": {"is_syncing": false}}""".toByteArray(), null)),
                    Mono.just(ChainResponse("""{"data": {"connected": "20"}}""".toByteArray(), null)),
                    UpstreamAvailability.UNAVAILABLE,
                ),
                Arguments.of(
                    Mono.just(ChainResponse(ByteArray(0), null)),
                    Mono.just(ChainResponse("""{"data": {"is_syncing": true}}""".toByteArray(), null)),
                    Mono.just(ChainResponse("""{"data": {"connected": "20"}}""".toByteArray(), null)),
                    UpstreamAvailability.SYNCING,
                ),
                Arguments.of(
                    Mono.just(ChainResponse(ByteArray(0), null)),
                    Mono.just(ChainResponse("""{"data": {"is_syncing": false}}""".toByteArray(), null)),
                    Mono.just(ChainResponse("""{"data": {"connected": "0"}}""".toByteArray(), null)),
                    UpstreamAvailability.IMMATURE,
                ),
                Arguments.of(
                    Mono.just(ChainResponse(ByteArray(0), null)),
                    Mono.error<ChainResponse>(RuntimeException("error")),
                    Mono.just(ChainResponse("""{"data": {"connected": "20"}}""".toByteArray(), null)),
                    UpstreamAvailability.UNAVAILABLE,
                ),
                Arguments.of(
                    Mono.just(ChainResponse(ByteArray(0), null)),
                    Mono.just(ChainResponse("""{"data": {"is_syncing": false}}""".toByteArray(), null)),
                    Mono.error<ChainResponse>(RuntimeException("error")),
                    UpstreamAvailability.UNAVAILABLE,
                ),
                Arguments.of(
                    Mono.just(ChainResponse(null, ChainCallError(1, "Error"))),
                    Mono.just(ChainResponse("""{"data": {"is_syncing": false}}""".toByteArray(), null)),
                    Mono.just(ChainResponse("""{"data": {"connected": "20"}}""".toByteArray(), null)),
                    UpstreamAvailability.UNAVAILABLE,
                ),
            )
        }
    }
}
