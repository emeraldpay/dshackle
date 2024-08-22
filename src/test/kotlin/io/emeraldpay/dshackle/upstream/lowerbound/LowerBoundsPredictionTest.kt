package io.emeraldpay.dshackle.upstream.lowerbound

import io.emeraldpay.dshackle.Chain
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

class LowerBoundsPredictionTest {

    @Test
    fun `first archival lower bound data, get it and predict the next bound`() {
        val lowerBounds = LowerBounds(Chain.ETHEREUM__MAINNET)
        val newLowerBound = LowerBoundData(1L, 1000, LowerBoundType.STATE)

        lowerBounds.updateBound(newLowerBound)

        val lastBound = lowerBounds.getLastBound(LowerBoundType.STATE)
        val predictedNextBound = lowerBounds.predictNextBound(LowerBoundType.STATE)
        val allBounds = lowerBounds.getAllBounds(LowerBoundType.STATE)

        assertThat(lastBound).isEqualTo(newLowerBound)
        assertThat(predictedNextBound).isEqualTo(1)
        assertThat(allBounds).isEqualTo(listOf(newLowerBound))
    }

    @Test
    fun `if no bound the default values`() {
        val lowerBounds = LowerBounds(Chain.ETHEREUM__MAINNET)

        val lastBound = lowerBounds.getLastBound(LowerBoundType.STATE)
        val predictedNextBound = lowerBounds.predictNextBound(LowerBoundType.STATE)
        val allBounds = lowerBounds.getAllBounds(LowerBoundType.STATE)

        assertThat(lastBound).isNull()
        assertThat(predictedNextBound).isEqualTo(0)
        assertThat(allBounds).isEmpty()
    }

    @Test
    fun `sequential archival lower bound data and get only the last`() {
        val lowerBounds = LowerBounds(Chain.ETHEREUM__MAINNET)
        val newLowerBound = LowerBoundData(1L, 1000, LowerBoundType.STATE)
        val nextNewLowerBound = LowerBoundData(1L, 1005, LowerBoundType.STATE)

        lowerBounds.updateBound(newLowerBound)

        val lastBound = lowerBounds.getLastBound(LowerBoundType.STATE)
        val predictedNextBound = lowerBounds.predictNextBound(LowerBoundType.STATE)
        val allBounds = lowerBounds.getAllBounds(LowerBoundType.STATE)

        assertThat(lastBound).isEqualTo(newLowerBound)
        assertThat(predictedNextBound).isEqualTo(1)
        assertThat(allBounds).isEqualTo(listOf(newLowerBound))

        lowerBounds.updateBound(nextNewLowerBound)

        val newLastBound = lowerBounds.getLastBound(LowerBoundType.STATE)
        val newPredictedNextBound = lowerBounds.predictNextBound(LowerBoundType.STATE)
        val newAllBounds = lowerBounds.getAllBounds(LowerBoundType.STATE)

        assertThat(newLastBound).isEqualTo(nextNewLowerBound)
        assertThat(newPredictedNextBound).isEqualTo(1)
        assertThat(newAllBounds).isEqualTo(listOf(nextNewLowerBound))
    }

    @Test
    fun `don't update the lower bounds if the same timestamp`() {
        val lowerBounds = LowerBounds(Chain.ETHEREUM__MAINNET)
        val newLowerBound = LowerBoundData(1L, 1000, LowerBoundType.STATE)

        lowerBounds.updateBound(newLowerBound)
        lowerBounds.updateBound(LowerBoundData(100000L, 1000, LowerBoundType.STATE))

        val lastBound = lowerBounds.getLastBound(LowerBoundType.STATE)
        val predictedNextBound = lowerBounds.predictNextBound(LowerBoundType.STATE)
        val allBounds = lowerBounds.getAllBounds(LowerBoundType.STATE)

        assertThat(lastBound).isEqualTo(newLowerBound)
        assertThat(predictedNextBound).isEqualTo(1)
        assertThat(allBounds).isEqualTo(listOf(newLowerBound))
    }

    @Test
    fun `always get the last bound`() {
        val lowerBounds = LowerBounds(Chain.ETHEREUM__MAINNET)
        val lowerBound1 = LowerBoundData(1000L, 1000, LowerBoundType.STATE)
        val lowerBound2 = LowerBoundData(1005L, 1005, LowerBoundType.STATE)
        val lowerBound3 = LowerBoundData(1010L, 1010, LowerBoundType.STATE)

        lowerBounds.updateBound(lowerBound1)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBound1)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBound1))

        lowerBounds.updateBound(lowerBound2)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBound2)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBound1, lowerBound2))

        lowerBounds.updateBound(lowerBound3)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBound3)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBound1, lowerBound2, lowerBound3))
    }

    @Test
    fun `preserve the maximum number of bounds`() {
        val lowerBounds = LowerBounds(Chain.ETHEREUM__MAINNET)
        val lowerBound1 = LowerBoundData(1000L, 1000, LowerBoundType.STATE)
        val lowerBound2 = LowerBoundData(1005L, 1005, LowerBoundType.STATE)
        val lowerBound3 = LowerBoundData(1010L, 1010, LowerBoundType.STATE)
        val lowerBound4 = LowerBoundData(1050L, 1050, LowerBoundType.STATE)
        val lowerBound5 = LowerBoundData(1060L, 1060, LowerBoundType.STATE)

        lowerBounds.updateBound(lowerBound1)
        lowerBounds.updateBound(lowerBound2)
        lowerBounds.updateBound(lowerBound3)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBound3)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBound1, lowerBound2, lowerBound3))

        lowerBounds.updateBound(lowerBound4)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBound4)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBound2, lowerBound3, lowerBound4))

        lowerBounds.updateBound(lowerBound5)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBound5)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBound3, lowerBound4, lowerBound5))
    }

    @Test
    fun `if get the archival bound then remove previous ones`() {
        val lowerBounds = LowerBounds(Chain.ETHEREUM__MAINNET)
        val lowerBound1 = LowerBoundData(1000L, 1000, LowerBoundType.STATE)
        val lowerBound2 = LowerBoundData(1005L, 1005, LowerBoundType.STATE)
        val lowerBound3 = LowerBoundData(1, 1010, LowerBoundType.STATE)

        lowerBounds.updateBound(lowerBound1)
        lowerBounds.updateBound(lowerBound2)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBound2)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBound1, lowerBound2))

        lowerBounds.updateBound(lowerBound3)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBound3)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBound3))
    }

    @Test
    fun `predict the same bound if all bounds are equal to each other`() {
        val lowerBounds = LowerBounds(Chain.ETHEREUM__MAINNET)
        val lowerBound1 = LowerBoundData(15060L, 1000, LowerBoundType.STATE)
        val lowerBound2 = LowerBoundData(15060L, 2000, LowerBoundType.STATE)
        val lowerBound3 = LowerBoundData(15060L, 3000, LowerBoundType.STATE)

        lowerBounds.updateBound(lowerBound1)
        lowerBounds.updateBound(lowerBound2)
        lowerBounds.updateBound(lowerBound3)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBound3)
        assertThat(lowerBounds.predictNextBound(LowerBoundType.STATE)).isEqualTo(15060L)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBound1, lowerBound2, lowerBound3))
    }

    @Test
    fun `predict the next bound based on different bounds`() {
        val now = Instant.now()
        val lowerBounds = LowerBounds(Chain.BSC__MAINNET)
        val lowerBound1 = LowerBoundData(37995846, now.minus(9, ChronoUnit.MINUTES).epochSecond, LowerBoundType.STATE)
        val lowerBound2 = LowerBoundData(37995906, now.minus(6, ChronoUnit.MINUTES).epochSecond, LowerBoundType.STATE)
        val lowerBound3 = LowerBoundData(37995966, now.minus(3, ChronoUnit.MINUTES).epochSecond, LowerBoundType.STATE)

        lowerBounds.updateBound(lowerBound1)
        lowerBounds.updateBound(lowerBound2)
        lowerBounds.updateBound(lowerBound3)

        val predicted = lowerBounds.predictNextBound(LowerBoundType.STATE)

        assertThat(predicted)
            .isLessThan(37996030)
            .isGreaterThan(37996020)
    }

    @Test
    fun `predict the next bound based on average speed`() {
        val now = Instant.now()
        val lowerBounds = LowerBounds(Chain.BSC__MAINNET)
        val lowerBound1 = LowerBoundData(37995966, now.minus(3, ChronoUnit.MINUTES).epochSecond, LowerBoundType.STATE)

        lowerBounds.updateBound(lowerBound1)

        val predicted = lowerBounds.predictNextBound(LowerBoundType.STATE)
        println(predicted)

        assertThat(predicted)
            .isLessThan(37996030)
            .isGreaterThan(37996020)
    }

    @Test
    fun `update different bounds`() {
        val lowerBounds = LowerBounds(Chain.ETHEREUM__MAINNET)
        val lowerBoundState1 = LowerBoundData(15060L, 1010, LowerBoundType.STATE)
        val lowerBoundState2 = LowerBoundData(16060L, 1020, LowerBoundType.STATE)
        val lowerBoundState3 = LowerBoundData(17060L, 1030, LowerBoundType.STATE)
        val lowerBoundBlock1 = LowerBoundData(20000, 1010, LowerBoundType.BLOCK)
        val lowerBoundBlock2 = LowerBoundData(21000, 1020, LowerBoundType.BLOCK)
        val lowerBoundBlock3 = LowerBoundData(22000, 1030, LowerBoundType.BLOCK)

        lowerBounds.updateBound(lowerBoundState1)
        lowerBounds.updateBound(lowerBoundState2)
        lowerBounds.updateBound(lowerBoundState3)
        lowerBounds.updateBound(lowerBoundBlock1)
        lowerBounds.updateBound(lowerBoundBlock2)
        lowerBounds.updateBound(lowerBoundBlock3)

        assertThat(lowerBounds.getLastBound(LowerBoundType.STATE)).isEqualTo(lowerBoundState3)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.STATE)).isEqualTo(listOf(lowerBoundState1, lowerBoundState2, lowerBoundState3))

        assertThat(lowerBounds.getLastBound(LowerBoundType.BLOCK)).isEqualTo(lowerBoundBlock3)
        assertThat(lowerBounds.getAllBounds(LowerBoundType.BLOCK)).isEqualTo(listOf(lowerBoundBlock1, lowerBoundBlock2, lowerBoundBlock3))
    }
}
