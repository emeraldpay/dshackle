package io.emeraldpay.dshackle.upstream.near

import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class NearUpstreamSettingsDetectorTest {

    @Test
    fun `detect client version`() {
        val reader = mock<ChainReader> {
            on { read(ChainRequest("status", ListParams())) } doReturn
                Mono.just(
                    ChainResponse(
                        """
                            {
                                "version": {
                                    "build": "1.38.1",
                                    "rustc_version": "1.75.0",
                                    "version": "1.38.1"
                                }
                            }
                        """.trimIndent().toByteArray(),
                        null,
                    ),
                )
        }
        val up = mock<Upstream> {
            on { getIngressReader() } doReturn reader
        }

        val detector = NearUpstreamSettingsDetector(up)

        StepVerifier.create(detector.detectClientVersion())
            .expectNext("1.38.1")
            .expectComplete()
            .verify()
    }
}
