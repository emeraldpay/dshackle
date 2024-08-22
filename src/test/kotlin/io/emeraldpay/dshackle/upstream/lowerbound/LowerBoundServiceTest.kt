package io.emeraldpay.dshackle.upstream.lowerbound

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.Upstream
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify

class LowerBoundServiceTest {

    @Test
    fun `predict lower bound`() {
        val detector = mock<LowerBoundDetector> {
            on { predictLowerBound(LowerBoundType.STATE) } doReturn 4000
            on { types() } doReturn setOf(LowerBoundType.STATE)
        }
        val boundService = LowerBoundServiceMock(mock<Upstream>(), listOf(detector))

        val bound = boundService.predictLowerBound(LowerBoundType.STATE)

        verify(detector).types()
        verify(detector).predictLowerBound(LowerBoundType.STATE)

        assertThat(bound).isEqualTo(4000)
    }

    @Test
    fun `the predicted lower bound is 0 if there is no such bound type`() {
        val detector = mock<LowerBoundDetector> {
            on { types() } doReturn setOf(LowerBoundType.STATE)
        }
        val boundService = LowerBoundServiceMock(mock<Upstream>(), listOf(detector))

        val bound = boundService.predictLowerBound(LowerBoundType.BLOCK)

        verify(detector).types()
        verify(detector, never()).predictLowerBound(any())

        assertThat(bound).isEqualTo(0)
    }

    private class LowerBoundServiceMock(
        upstream: Upstream,
        private val detectors: List<LowerBoundDetector>,
    ) : LowerBoundService(Chain.ETHEREUM__MAINNET, upstream) {

        override fun detectors(): List<LowerBoundDetector> {
            return detectors
        }
    }
}
