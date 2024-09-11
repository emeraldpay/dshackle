package io.emeraldpay.dshackle.upstream.polkadot

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.core.publisher.Mono

class BasicPolkadotUpstreamRpcMethodsDetectorTest {
    @Test
    fun `rpc_methods enabled`() {
        val reader =
            mock<ChainReader> {
                on {
                    read(ChainRequest("rpc_methods", ListParams()))
                } doReturn
                    Mono.just(
                        ChainResponse(
                            """ {"methods": ["account_nextIndex","archive_unstable_body","archive_unstable_call"]}"""
                                .toByteArray(),
                            null,
                        ),
                    )
            }

        val upstream =
            mock<Upstream> {
                on { getIngressReader() } doReturn reader
                on { getChain() } doReturn Chain.POLKADOT__MAINNET
            }
        val detector = BasicPolkadotUpstreamRpcMethodsDetector(upstream)
        Assertions.assertThat(detector.detectRpcMethods().block()).apply {
            isNotNull()
            hasSize(3)
            containsKeys("account_nextIndex", "archive_unstable_body", "archive_unstable_call")
        }
    }

    @Test
    fun `rpc_methods disabled`() {
        val reader =
            mock<ChainReader> {
                on {
                    read(ChainRequest("rpc_methods", ListParams()))
                } doReturn
                    Mono.just(
                        ChainResponse(
                            null,
                            ChainCallError(32601, "the method rpc_methods does not exist/is not available"),
                        ),
                    )
            }

        val upstream =
            mock<Upstream> {
                on { getIngressReader() } doReturn reader
                on { getChain() } doReturn Chain.POLKADOT__MAINNET
            }
        val detector = BasicPolkadotUpstreamRpcMethodsDetector(upstream)
        Assertions.assertThat(detector.detectRpcMethods().block()).isNull()
    }
}
