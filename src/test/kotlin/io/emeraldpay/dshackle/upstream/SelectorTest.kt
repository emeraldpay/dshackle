package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.BlockchainOuterClass.BlockTag
import io.emeraldpay.api.proto.BlockchainOuterClass.HeightSelector
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
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

    @ParameterizedTest
    @MethodSource("finalData")
    fun `sort with finalization`(
        finalizationType: FinalizationType,
        finalizationProto: BlockchainOuterClass.BlockTag,
    ) {
        val up1 = mock<Upstream> {
            on { getFinalizations() } doReturn listOf(FinalizationData(1L, finalizationType))
        }
        val up2 = mock<Upstream> {
            on { getFinalizations() } doReturn listOf(FinalizationData(10L, finalizationType))
        }
        val up3 = mock<Upstream> {
            on { getFinalizations() } doReturn listOf(FinalizationData(100L, finalizationType))
        }
        val up4 = mock<Upstream> {
            on { getFinalizations() } doReturn listOf()
        }
        val ups = listOf(up4, up3, up2, up1)
        val requestSelectors = listOf(
            BlockchainOuterClass.Selector.newBuilder()
                .setHeightSelector(
                    HeightSelector.newBuilder()
                        .setTag(finalizationProto),
                )
                .build(),
        )

        val upstreamFilter = Selector.convertToUpstreamFilter(requestSelectors)

        val actual = ups.sortedWith(upstreamFilter.sort.comparator)

        assertEquals(
            listOf(up3, up2, up1, up4),
            actual,
        )
    }

    @Test
    fun `sort with latest`() {
        val mockHead1 = mock<Head> {
            on { getCurrentHeight() } doReturn 1L
        }
        val up1 = mock<Upstream> {
            on { getHead() } doReturn mockHead1
        }

        val mockHead2 = mock<Head> {
            on { getCurrentHeight() } doReturn 2L
        }
        val up2 = mock<Upstream> {
            on { getHead() } doReturn mockHead2
        }

        val mockHead3 = mock<Head> {
            on { getCurrentHeight() } doReturn 3L
        }
        val up3 = mock<Upstream> {
            on { getHead() } doReturn mockHead3
        }

        val mockHead4 = mock<Head> {
            on { getCurrentHeight() } doReturn null
        }
        val up4 = mock<Upstream> {
            on { getHead() } doReturn mockHead4
        }

        val ups = listOf(up2, up1, up4, up3)
        val requestSelectors = listOf(
            BlockchainOuterClass.Selector.newBuilder()
                .setHeightSelector(
                    HeightSelector.newBuilder()
                        .setTag(BlockTag.LATEST),
                )
                .build(),
        )

        val upstreamFilter = Selector.convertToUpstreamFilter(requestSelectors)

        val actual = ups.sortedWith(upstreamFilter.sort.comparator)

        assertEquals(
            listOf(up3, up2, up1, up4),
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
                        .setHeight(100050003)
                        .setLowerBoundType(BlockchainOuterClass.LowerBoundType.LOWER_BOUND_BLOCK)
                        .build(),
                )
                .build(),
        )

        val upstreamFilter = Selector.convertToUpstreamFilter(requestSelectors)

        val actual = ups.sortedWith(upstreamFilter.sort.comparator)
        val actualMatcher = Selector.MultiMatcher(listOf(Selector.LowerHeightMatcher(100050003, LowerBoundType.BLOCK)))

        assertEquals(
            upstreamFilter.matcher,
            actualMatcher,
        )
        assertEquals(
            listOf(up1, up3, up2),
            actual,
        )
    }

    @ParameterizedTest
    @MethodSource("lowerHeightData")
    fun `test lower height matcher`(
        lowerHeight: Long,
        predicted: Long,
        expected: MatchesResponse,
    ) {
        val up = mock<Upstream> {
            on { predictLowerBound(LowerBoundType.STATE) } doReturn predicted
        }
        val matcher = Selector.LowerHeightMatcher(lowerHeight, LowerBoundType.STATE)

        val actualResponse = matcher.matchesWithCause(up)

        assertEquals(expected, actualResponse)
    }

    companion object {
        @JvmStatic
        fun lowerHeightData(): List<Arguments> =
            listOf(
                of(10000, 400, MatchesResponse.Success),
                of(10000, 50000, MatchesResponse.LowerHeightResponse(10000, 50000, LowerBoundType.STATE)),
                of(5000, 5000, MatchesResponse.Success),
                of(3000, 0, MatchesResponse.LowerHeightResponse(3000, 0, LowerBoundType.STATE)),
            )

        @JvmStatic
        fun data(): List<Arguments> =
            listOf(
                of(LowerBoundType.STATE, BlockchainOuterClass.LowerBoundType.LOWER_BOUND_STATE),
                of(LowerBoundType.BLOCK, BlockchainOuterClass.LowerBoundType.LOWER_BOUND_BLOCK),
                of(LowerBoundType.SLOT, BlockchainOuterClass.LowerBoundType.LOWER_BOUND_SLOT),
            )

        @JvmStatic
        fun finalData(): List<Arguments> =
            listOf(
                of(FinalizationType.SAFE_BLOCK, BlockTag.SAFE),
                of(FinalizationType.FINALIZED_BLOCK, BlockTag.FINALIZED),
            )
    }
}
