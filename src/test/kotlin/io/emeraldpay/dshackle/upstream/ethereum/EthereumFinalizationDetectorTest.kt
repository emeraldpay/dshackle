package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionRefJson
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration
import java.time.Instant

class EthereumFinalizationDetectorTest {

    @Test
    fun testDetectFinalization() {
        val reader = mock<ChainReader> {
            on {
                read(ChainRequest("eth_getBlockByNumber", ListParams("safe", false), 1))
            } doReturn response(1) doReturn response(5) doReturn response(3)
            on {
                read(ChainRequest("eth_getBlockByNumber", ListParams("finalized", false), 2))
            } doReturn response(2) doReturn response(10) doReturn response(5)
        }
        val upstream = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }
        val detector = EthereumFinalizationDetector()

        StepVerifier.withVirtualTime { detector.detectFinalization(upstream, Duration.ofMillis(200)) }
            .expectSubscription()
            .thenAwait(Duration.ofSeconds(0))
            .expectNext(FinalizationData(1L, FinalizationType.SAFE_BLOCK))
            .expectNext(FinalizationData(2L, FinalizationType.FINALIZED_BLOCK))
            .thenAwait(Duration.ofSeconds(15))
            .expectNext(FinalizationData(5L, FinalizationType.SAFE_BLOCK))
            .expectNext(FinalizationData(10L, FinalizationType.FINALIZED_BLOCK))
            .thenAwait(Duration.ofSeconds(15))
            .expectNoEvent(Duration.ofMillis(100))
            .thenCancel()
            .verify(Duration.ofSeconds(1))
    }

    private fun response(blockNumber: Long) =
        Mono.just(
            ChainResponse(
                Global.objectMapper.writeValueAsString(
                    BlockJson<TransactionRefJson>().apply {
                        number = blockNumber
                        timestamp = Instant.now()
                    },
                ).toByteArray(),
                null,
            ),
        )
}
