package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainCallError
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
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
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

        val chain = mock<Chain> {
            on { toString() } doReturn "TestChain"
        }
        val detector = EthereumFinalizationDetector()

        StepVerifier.withVirtualTime { detector.detectFinalization(upstream, Duration.ofMillis(200), chain) }
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

    @Test
    fun `safe and finalized blocks returns null`() {
        val reader = mock<ChainReader> {
            on {
                read(ChainRequest("eth_getBlockByNumber", ListParams("safe", false), 1))
            } doReturn Mono.just(ChainResponse(Global.nullValue, null)) doReturn Mono.just(ChainResponse(Global.nullValue, null))
            on {
                read(ChainRequest("eth_getBlockByNumber", ListParams("finalized", false), 2))
            } doReturn Mono.just(ChainResponse(Global.nullValue, null)) doReturn Mono.just(ChainResponse(Global.nullValue, null))
        }
        val upstream = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }

        val detector = EthereumFinalizationDetector()

        StepVerifier.withVirtualTime { detector.detectFinalization(upstream, Duration.ofSeconds(10), Chain.ZKSYNC__MAINNET) }
            .expectSubscription()
            .thenAwait(Duration.ofSeconds(0))
            .expectNoEvent(Duration.ofMillis(50))
            .thenAwait(Duration.ofSeconds(15))
            .expectNoEvent(Duration.ofMillis(50))
            .thenCancel()
            .verify(Duration.ofSeconds(1))

        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("safe", false), 1))
        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("finalized", false), 2))
    }

    @Test
    fun `disable detector if receive specific errors`() {
        val reader = mock<ChainReader> {
            on {
                read(ChainRequest("eth_getBlockByNumber", ListParams("safe", false), 1))
            } doReturn Mono.just(ChainResponse(null, ChainCallError(1, "Got an invalid block number, check it")))
            on {
                read(ChainRequest("eth_getBlockByNumber", ListParams("finalized", false), 2))
            } doReturn response(2) doReturn response(10)
        }

        val upstream = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }

        val detector = EthereumFinalizationDetector()

        StepVerifier.withVirtualTime { detector.detectFinalization(upstream, Duration.ofSeconds(10), Chain.ZKSYNC__MAINNET) }
            .expectSubscription()
            .thenAwait(Duration.ofSeconds(0))
            .expectNext(FinalizationData(2L, FinalizationType.FINALIZED_BLOCK))
            .thenAwait(Duration.ofSeconds(15))
            .expectNext(FinalizationData(10L, FinalizationType.FINALIZED_BLOCK))
            .thenCancel()
            .verify(Duration.ofSeconds(1))

        verify(reader).read(ChainRequest("eth_getBlockByNumber", ListParams("safe", false), 1))
        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("finalized", false), 2))
    }

    @Test
    fun `ignore response if it cannot be parsed`() {
        val reader = mock<ChainReader> {
            on {
                read(ChainRequest("eth_getBlockByNumber", ListParams("safe", false), 1))
            } doReturn Mono.just(ChainResponse("""{"name": "value"}""".toByteArray(), null)) doReturn Mono.just(ChainResponse("""{"name": "value"}""".toByteArray(), null))
            on {
                read(ChainRequest("eth_getBlockByNumber", ListParams("finalized", false), 2))
            } doReturn response(2) doReturn response(10)
        }
        val upstream = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }

        val detector = EthereumFinalizationDetector()

        StepVerifier.withVirtualTime { detector.detectFinalization(upstream, Duration.ofSeconds(10), Chain.ZKSYNC__MAINNET) }
            .expectSubscription()
            .thenAwait(Duration.ofSeconds(0))
            .expectNext(FinalizationData(2L, FinalizationType.FINALIZED_BLOCK))
            .thenAwait(Duration.ofSeconds(15))
            .expectNext(FinalizationData(10L, FinalizationType.FINALIZED_BLOCK))
            .thenCancel()
            .verify(Duration.ofSeconds(1))

        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("safe", false), 1))
        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("finalized", false), 2))
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
