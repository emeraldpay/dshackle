package io.emeraldpay.dshackle.upstream.solana

import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.UNKNOWN_CLIENT_VERSION
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class SolanaUpstreamSettingsDetectorTest {

    @Test
    fun `detect client version`() {
        val reader = mock<ChainReader> {
            on { read(ChainRequest("getVersion", ListParams())) } doReturn
                Mono.just(
                    ChainResponse(
                        """
                            {
                                "feature-set": 2891131721,
                                "solana-core": "1.16.7"
                            } 
                        """.trimIndent().toByteArray(),
                        null,
                    ),
                )
        }
        val up = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }

        val detector = SolanaUpstreamSettingsDetector(up)

        StepVerifier.create(detector.detectClientVersion())
            .expectNext("1.16.7")
            .expectComplete()
            .verify()
    }

    @Test
    fun `unknown client if there is an error`() {
        val reader = mock<ChainReader> {
            on { read(ChainRequest("getVersion", ListParams())) } doReturn
                Mono.just(
                    ChainResponse(
                        null,
                        ChainCallError(
                            1,
                            "message",
                        ),
                    ),
                )
        }
        val up = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }

        val detector = SolanaUpstreamSettingsDetector(up)

        StepVerifier.create(detector.detectClientVersion())
            .expectNext(UNKNOWN_CLIENT_VERSION)
            .expectComplete()
            .verify()
    }
}
