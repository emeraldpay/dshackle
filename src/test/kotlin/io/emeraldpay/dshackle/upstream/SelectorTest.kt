package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.of
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock

class SelectorTest {

    @ParameterizedTest
    @MethodSource("data")
    fun `sort with lower height matcher`(
        lowerBoundType: LowerBoundType,
        protoLowerBoundType: BlockchainOuterClass.LowerBoundType,
    ) {
        val up1 = mock<Upstream> {
            on { getLowerBound(lowerBoundType) } doReturn LowerBoundData(1, lowerBoundType)
        }
        val up2 = mock<Upstream> {
            on { getLowerBound(lowerBoundType) } doReturn LowerBoundData(1000, lowerBoundType)
        }
        val up3 = mock<Upstream> {
            on { getLowerBound(lowerBoundType) } doReturn LowerBoundData(100000, lowerBoundType)
        }
        val up4 = mock<Upstream> {
            on { getLowerBound(lowerBoundType) } doReturn null
        }
        val ups = listOf(up4, up3, up2, up1)
        val requestSelectors = listOf(
            BlockchainOuterClass.Selector.newBuilder()
                .setLowerHeightSelector(
                    BlockchainOuterClass.LowerHeightSelector.newBuilder()
                        .setLowerBoundType(protoLowerBoundType)
                        .build(),
                )
                .build(),
        )

        val upstreamFilter = Selector.convertToUpstreamFilter(requestSelectors)

        val actual = ups.sortedWith(upstreamFilter.sort.comparator)

        assertEquals(
            listOf(up1, up2, up3, up4),
            actual,
        )
    }

    @Test
    fun `preserve the same order if no lower bound type`() {
        val up1 = mock<Upstream> {
            on { getLowerBound(LowerBoundType.STATE) } doReturn LowerBoundData(1, LowerBoundType.STATE)
        }
        val up2 = mock<Upstream> {
            on { getLowerBound(LowerBoundType.STATE) } doReturn LowerBoundData(1000, LowerBoundType.STATE)
        }
        val up3 = mock<Upstream> {
            on { getLowerBound(LowerBoundType.STATE) } doReturn LowerBoundData(100000, LowerBoundType.STATE)
        }
        val up4 = mock<Upstream> {
            on { getLowerBound(LowerBoundType.STATE) } doReturn null
        }
        val ups = listOf(up4, up3, up2, up1)
        val requestSelectors = listOf(
            BlockchainOuterClass.Selector.newBuilder()
                .setLowerHeightSelector(
                    BlockchainOuterClass.LowerHeightSelector.newBuilder()
                        .build(),
                )
                .build(),
        )

        val upstreamFilter = Selector.convertToUpstreamFilter(requestSelectors)

        val actual = ups.sortedWith(upstreamFilter.sort.comparator)

        assertEquals(
            listOf(up4, up3, up2, up1),
            actual,
        )
    }

    @Test
    fun `ignore upstream with no needed lower bound`() {
        val up1 = mock<Upstream> {
            on { getLowerBound(LowerBoundType.STATE) } doReturn LowerBoundData(1, LowerBoundType.STATE)
        }
        val up2 = mock<Upstream> {
            on { getLowerBound(LowerBoundType.BLOCK) } doReturn LowerBoundData(1000, LowerBoundType.BLOCK)
        }
        val up3 = mock<Upstream> {
            on { getLowerBound(LowerBoundType.BLOCK) } doReturn LowerBoundData(100000, LowerBoundType.BLOCK)
        }
        val ups = listOf(up1, up3, up2)
        val requestSelectors = listOf(
            BlockchainOuterClass.Selector.newBuilder()
                .setLowerHeightSelector(
                    BlockchainOuterClass.LowerHeightSelector.newBuilder()
                        .setLowerBoundType(BlockchainOuterClass.LowerBoundType.LOWER_BOUND_BLOCK)
                        .build(),
                )
                .build(),
        )

        val upstreamFilter = Selector.convertToUpstreamFilter(requestSelectors)

        val actual = ups.sortedWith(upstreamFilter.sort.comparator)

        assertEquals(
            listOf(up2, up3, up1),
            actual,
        )
    }

    companion object {
        @JvmStatic
        fun data(): List<Arguments> =
            listOf(
                of(LowerBoundType.STATE, BlockchainOuterClass.LowerBoundType.LOWER_BOUND_STATE),
                of(LowerBoundType.BLOCK, BlockchainOuterClass.LowerBoundType.LOWER_BOUND_BLOCK),
                of(LowerBoundType.SLOT, BlockchainOuterClass.LowerBoundType.LOWER_BOUND_SLOT),
            )
    }
}
