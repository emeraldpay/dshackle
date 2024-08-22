package io.emeraldpay.dshackle.upstream.lowerbound

import com.google.common.util.concurrent.AtomicDouble
import io.emeraldpay.dshackle.Chain
import org.apache.commons.math3.stat.regression.SimpleRegression
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.math.roundToLong

class LowerBounds(
    chain: Chain,
) {
    companion object {
        private const val MAX_BOUNDS = 3
    }

    private val averageSpeed = chain.averageRemoveDataSpeed

    private val lowerBounds = ConcurrentHashMap<LowerBoundType, LowerBoundCoeffs>()

    fun updateBound(newBound: LowerBoundData) {
        if (lowerBounds.containsKey(newBound.type)) {
            val lowerBoundCoeffs = lowerBounds[newBound.type]!!

            // we add only bounds with different timestamps
            if (newBound.timestamp != lowerBoundCoeffs.getLastBound().timestamp) {
                if (newBound.lowerBound == 1L) {
                    // this is the fully archival node, so there is no need to accumulate bounds and calculate the coeffs
                    lowerBoundCoeffs.updateCoeffs(0.0, 1.0)
                    lowerBoundCoeffs.clearBounds()
                    lowerBoundCoeffs.addBound(newBound)
                } else {
                    // accumulate up to MAX_BOUNDS and preserve this size
                    if (lowerBoundCoeffs.boundsSize() == MAX_BOUNDS) {
                        lowerBoundCoeffs.removeFirst()
                    }
                    lowerBoundCoeffs.addBound(newBound)
                    if (lowerBoundCoeffs.boundsSize() < MAX_BOUNDS) {
                        // calculate coeffs based on the average speed until we accumulate al least MAX_BOUNDS bounds
                        lowerBoundCoeffs.updateCoeffs(averageSpeed, calculateB(newBound))
                    } else {
                        // having MAX_BOUNDS bounds we can use linear regression
                        lowerBoundCoeffs.train()
                    }
                }
            }
        } else {
            // add new bound if it hasn't existed yet
            lowerBounds[newBound.type] = LowerBoundCoeffs()
                .apply {
                    addBound(newBound)
                    if (newBound.lowerBound == 1L) {
                        // this is the fully archival node
                        updateCoeffs(0.0, 1.0)
                    } else {
                        // otherwise we calculate the coeffs based on the average speed
                        updateCoeffs(averageSpeed, calculateB(newBound))
                    }
                }
        }
    }

    fun predictNextBound(type: LowerBoundType): Long {
        val lowerBoundCoeffs = lowerBounds[type] ?: return 0

        val xTime = Instant.now().epochSecond

        return (lowerBoundCoeffs.k.get() * xTime + lowerBoundCoeffs.b.get()).roundToLong()
    }

    fun getLastBound(type: LowerBoundType): LowerBoundData? {
        return lowerBounds[type]?.getLastBound()
    }

    fun getAllBounds(type: LowerBoundType): List<LowerBoundData> {
        return lowerBounds[type]?.lowerBounds?.toList() ?: emptyList()
    }

    private fun calculateB(bound: LowerBoundData): Double {
        return bound.lowerBound.toDouble() - (averageSpeed * bound.timestamp)
    }

    // to predict the next lower bound we use linear regression, y = kx + b,
    // where x - current time, y - the predicted bound, k and b - coefficients
    // to achieve that we accumulate up to max bounds (3 by default) and then calculate the coefficients using the regression lib
    // having these coeffs we can predict the next bound in the predictNextBound() method
    private class LowerBoundCoeffs {
        val lowerBounds = ConcurrentLinkedDeque<LowerBoundData>()
        val k = AtomicDouble()
        val b = AtomicDouble()

        fun addBound(bound: LowerBoundData) {
            lowerBounds.add(bound)
        }

        fun updateCoeffs(newK: Double, newB: Double) {
            k.set(newK)
            b.set(newB)
        }

        fun clearBounds() {
            lowerBounds.clear()
        }

        fun removeFirst() {
            lowerBounds.removeFirst()
        }

        fun boundsSize(): Int = lowerBounds.size

        fun getLastBound(): LowerBoundData = lowerBounds.last

        fun train() {
            val regression = SimpleRegression()

            lowerBounds.forEach {
                regression.addObservation(doubleArrayOf(it.timestamp.toDouble()), it.lowerBound.toDouble())
            }

            updateCoeffs(regression.slope, regression.intercept)
        }
    }
}
