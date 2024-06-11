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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant

class EthereumFinalizationDetectorTest {

    private lateinit var upstream: Upstream
    private lateinit var chainReader: ChainReader
    private lateinit var detector: EthereumFinalizationDetector

    @BeforeEach
    fun setUp() {
        upstream = mock()
        chainReader = mock()
        `when`(upstream.getIngressReader()).thenReturn(chainReader)
        detector = EthereumFinalizationDetector()
    }

    @Test
    fun testDetectFinalization() {
        `when`(chainReader.read(ChainRequest("eth_getBlockByNumber", ListParams("safe", false), 1)))
            .thenReturn(
                Mono.just(
                    ChainResponse(
                        Global.objectMapper.writeValueAsString(
                            BlockJson<TransactionRefJson>().apply {
                                number = 1
                                timestamp = Instant.now()
                            },
                        ).toByteArray(),
                        null,
                    ),
                ),
            )
        `when`(chainReader.read(ChainRequest("eth_getBlockByNumber", ListParams("finalized", false), 2)))
            .thenReturn(
                Mono.just(
                    ChainResponse(
                        Global.objectMapper.writeValueAsString(
                            BlockJson<TransactionRefJson>().apply {
                                number = 2
                                timestamp = Instant.now()
                            },
                        ).toByteArray(),
                        null,
                    ),
                ),
            )

        val flux = detector.detectFinalization(upstream, Duration.ofMillis(200))
        flux.take(2).collectList().block()
        val result = detector.getFinalizations().toList()
        Assertions.assertEquals(2, result.size)
        org.assertj.core.api.Assertions.assertThat(result)
            .contains(FinalizationData(2L, FinalizationType.FINALIZED_BLOCK))
            .contains(FinalizationData(1L, FinalizationType.SAFE_BLOCK))
    }
}
