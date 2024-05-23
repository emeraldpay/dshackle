package io.emeraldpay.dshackle.upstream.error

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.verify

class UpstreamErrorHandlerTest {

    @Test
    fun `use lower bound error handler`() {
        val upstream = mock<Upstream>()
        val request = ChainRequest("eth_getCode", ListParams("0x343", "0xCB5A0A8"))
        val handler = UpstreamErrorHandler

        handler.handle(upstream, request, "missing trie node d5648cc9aef48154159d53800f2f")

        Thread.sleep(100)

        verify(upstream).updateLowerBound(213229736, LowerBoundType.STATE)
    }
}
