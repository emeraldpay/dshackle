package io.emeraldpay.dshackle.upstream.ton

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration

class TonChainIdValidatorTest {

    @ParameterizedTest
    @MethodSource("data")
    fun `validate ton chainId`(
        blockFilePath: String,
        expectedResult: ValidateUpstreamSettingsResult,
    ) {
        val masterchainInfoBytes = this::class.java.getResource("/blocks/ton/masterchain-info.json")!!.readBytes()
        val block = this::class.java.getResource(blockFilePath)!!.readBytes()
        val blockReq = ChainRequest(
            "GET#/getBlockHeader",
            RestParams(
                emptyList(),
                listOf(
                    "workchain" to "-1",
                    "shard" to "-9223372036854775808",
                    "seqno" to "41689287",
                ),
                emptyList(),
                ByteArray(0),
            ),
        )
        val reader = mock<ChainReader> {
            on { read(ChainRequest("GET#/getMasterchainInfo", RestParams.emptyParams())) } doReturn
                Mono.just(ChainResponse(masterchainInfoBytes, null))
            on { read(blockReq) } doReturn Mono.just(ChainResponse(block, null))
        }
        val upstream = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }
        val validator = TonChainIdValidator(upstream, Chain.TON__MAINNET)

        StepVerifier.create(validator.validate(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR))
            .expectSubscription()
            .expectNext(expectedResult)
            .expectComplete()
            .verify(Duration.ofSeconds(1))

        verify(reader).read(ChainRequest("GET#/getMasterchainInfo", RestParams.emptyParams()))
        verify(reader).read(blockReq)
    }

    @Test
    fun `setting error if couldn't read from upstream getMasterchainInfo`() {
        val reader = mock<ChainReader> {
            on { read(ChainRequest("GET#/getMasterchainInfo", RestParams.emptyParams())) } doReturn
                Mono.just(ChainResponse(null, ChainCallError(1, "Big error")))
        }
        val upstream = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }
        val validator = TonChainIdValidator(upstream, Chain.TON__MAINNET)

        StepVerifier.create(validator.validate(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR))
            .expectSubscription()
            .expectNext(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR)
            .expectComplete()
            .verify(Duration.ofSeconds(1))

        verify(reader).read(ChainRequest("GET#/getMasterchainInfo", RestParams.emptyParams()))
        verify(reader, never()).read(argThat { method == "GET#/getBlockHeader" })
    }

    @Test
    fun `setting error if couldn't read from upstream getBlockHeader`() {
        val masterchainInfoBytes = this::class.java.getResource("/blocks/ton/masterchain-info.json")!!.readBytes()
        val blockReq = ChainRequest(
            "GET#/getBlockHeader",
            RestParams(
                emptyList(),
                listOf(
                    "workchain" to "-1",
                    "shard" to "-9223372036854775808",
                    "seqno" to "41689287",
                ),
                emptyList(),
                ByteArray(0),
            ),
        )
        val reader = mock<ChainReader> {
            on { read(ChainRequest("GET#/getMasterchainInfo", RestParams.emptyParams())) } doReturn
                Mono.just(ChainResponse(masterchainInfoBytes, null))
            on { read(blockReq) } doReturn Mono.just(ChainResponse(null, ChainCallError(1, "Super error")))
        }
        val upstream = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }
        val validator = TonChainIdValidator(upstream, Chain.TON__MAINNET)

        StepVerifier.create(validator.validate(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR))
            .expectSubscription()
            .expectNext(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR)
            .expectComplete()
            .verify(Duration.ofSeconds(1))

        verify(reader).read(ChainRequest("GET#/getMasterchainInfo", RestParams.emptyParams()))
        verify(reader).read(blockReq)
    }

    companion object {
        @JvmStatic
        fun data(): List<Arguments> {
            return listOf(
                Arguments.of(
                    "/blocks/ton/ton-block.json",
                    ValidateUpstreamSettingsResult.UPSTREAM_VALID,
                ),
                Arguments.of(
                    "/blocks/ton/ton-block-wrong.json",
                    ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR,
                ),
                Arguments.of(
                    "/blocks/ton/ton-block-missed-global-id.json",
                    ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR,
                ),
            )
        }
    }
}
