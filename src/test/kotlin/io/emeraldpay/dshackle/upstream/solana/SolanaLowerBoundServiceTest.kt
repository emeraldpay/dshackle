package io.emeraldpay.dshackle.upstream.solana

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration

class SolanaLowerBoundServiceTest {

    @Test
    fun `get solana lower block and slot`() {
        val reader = mock<ChainReader> {
            on { read(ChainRequest("getFirstAvailableBlock", ListParams())) } doReturn
                Mono.just(ChainResponse("25000000".toByteArray(), null))
            on {
                read(
                    ChainRequest(
                        "getBlock",
                        ListParams(
                            25000000L,
                            mapOf(
                                "showRewards" to false,
                                "transactionDetails" to "none",
                                "maxSupportedTransactionVersion" to 0,
                            ),
                        ),
                    ),
                )
            } doReturn Mono.just(
                ChainResponse(
                    Global.objectMapper.writeValueAsBytes(
                        mapOf(
                            "blockHeight" to 21000000,
                            "blockTime" to 111,
                            "blockhash" to "22",
                            "previousBlockhash" to "33",
                        ),
                    ),
                    null,
                ),
            )
        }
        val upstream = mock<Upstream> {
            on { getIngressReader() } doReturn reader
            on { getChain() } doReturn Chain.UNSPECIFIED
        }

        val detector = SolanaLowerBoundService(Chain.UNSPECIFIED, upstream)

        StepVerifier.withVirtualTime { detector.detectLowerBounds() }
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(15))
            .expectNextMatches { it.lowerBound == 21000000L && it.type == LowerBoundType.STATE }
            .expectNextMatches { it.lowerBound == 25000000L && it.type == LowerBoundType.SLOT }
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        assertThat(detector.getLowerBounds().toList())
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
            .hasSameElementsAs(
                listOf(
                    LowerBoundData(21000000L, LowerBoundType.STATE),
                    LowerBoundData(25000000L, LowerBoundType.SLOT),
                ),
            )
    }
}
