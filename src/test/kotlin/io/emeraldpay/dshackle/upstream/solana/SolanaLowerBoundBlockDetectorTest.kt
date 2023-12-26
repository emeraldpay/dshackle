package io.emeraldpay.dshackle.upstream.solana

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.LowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration

class SolanaLowerBoundBlockDetectorTest {

    @Test
    fun `get solana lower block and slot`() {
        val reader = mock<JsonRpcReader> {
            on { read(JsonRpcRequest("getFirstAvailableBlock", listOf())) } doReturn
                Mono.just(JsonRpcResponse("25000000".toByteArray(), null))
            on { read(JsonRpcRequest("getBlocks", listOf(24999990L, 25000000L))) } doReturn
                Mono.just(JsonRpcResponse("[23000000, 23000005, 23000010]".toByteArray(), null))
            on {
                read(
                    JsonRpcRequest(
                        "getBlock",
                        listOf(
                            23000010L,
                            mapOf(
                                "showRewards" to false,
                                "transactionDetails" to "none",
                                "maxSupportedTransactionVersion" to 0,
                            ),
                        ),
                    ),
                )
            } doReturn Mono.just(
                JsonRpcResponse(
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
        }

        val detector = SolanaLowerBoundBlockDetector(Chain.UNSPECIFIED, upstream)

        StepVerifier.withVirtualTime { detector.lowerBlock() }
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(15))
            .expectNext(LowerBoundBlockDetector.LowerBlockData(21000000, 23000010))
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        Assertions.assertEquals(LowerBoundBlockDetector.LowerBlockData(21000000, 23000010), detector.getCurrentLowerBlock())
    }
}
