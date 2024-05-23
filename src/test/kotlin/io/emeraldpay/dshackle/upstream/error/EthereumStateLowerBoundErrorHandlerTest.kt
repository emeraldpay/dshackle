package io.emeraldpay.dshackle.upstream.error

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.of
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito.anyLong
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.never
import org.mockito.kotlin.verify

class EthereumStateLowerBoundErrorHandlerTest {

    @ParameterizedTest
    @MethodSource("requests")
    fun `update lower bound`(request: ChainRequest) {
        val upstream = mock<Upstream>()
        val handler = EthereumStateLowerBoundErrorHandler

        handler.handle(upstream, request, "missing trie node d5648cc9aef48154159d53800f2f")

        verify(upstream).updateLowerBound(213229736, LowerBoundType.STATE)
    }

    @Test
    fun `no update lower bound if error is not about state`() {
        val upstream = mock<Upstream>()
        val request = ChainRequest(
            "eth_getTransactionCount",
            ListParams("0x343", "0xCB5A0A8"),
        )
        val handler = EthereumStateLowerBoundErrorHandler

        handler.handle(upstream, request, "strange error")

        verify(upstream, never()).updateLowerBound(anyLong(), any())
    }

    @Test
    fun `no update lower bound if there is a non-state method`() {
        val upstream = mock<Upstream>()
        val request = ChainRequest(
            "eth_getBlockByNumber",
            ListParams("0xCB5A0A8", true),
        )
        val handler = EthereumStateLowerBoundErrorHandler

        handler.handle(upstream, request, "missing trie node d5648cc9aef48154159d53800f2f")

        verify(upstream, never()).updateLowerBound(anyLong(), any())
    }

    companion object {
        @JvmStatic
        fun requests(): List<Arguments> =
            listOf(
                of(ChainRequest("eth_getTransactionCount", ListParams("0x343", "0xCB5A0A8"))),
                of(ChainRequest("eth_getCode", ListParams("0x343", "0xCB5A0A8"))),
                of(ChainRequest("eth_estimateGas", ListParams("0x343", "0xCB5A0A8"))),
                of(ChainRequest("eth_getBalance", ListParams("0x343", "0xCB5A0A8"))),
                of(ChainRequest("debug_traceCall", ListParams("0x343", "0xCB5A0A8"))),
                of(ChainRequest("eth_call", ListParams("0x343", "0xCB5A0A8"))),
                of(ChainRequest("eth_getProof", ListParams("0x343", "test", "0xCB5A0A8"))),
                of(ChainRequest("eth_getStorageAt", ListParams("0x343", "test", "0xCB5A0A8"))),
            )
    }
}
