package io.emeraldpay.dshackle.upstream.lowerbound

import java.time.Instant

data class LowerBoundData(
    val lowerBound: Long,
    val timestamp: Long,
    val type: LowerBoundType,
) : Comparable<LowerBoundData> {
    constructor(lowerBound: Long, type: LowerBoundType) : this(lowerBound, Instant.now().epochSecond, type)

    companion object {
        fun default() = LowerBoundData(0, 0, LowerBoundType.UNKNOWN)
    }

    override fun compareTo(other: LowerBoundData): Int {
        return this.lowerBound.compareTo(other.lowerBound)
    }
}

enum class LowerBoundType {
    UNKNOWN, STATE, SLOT
}
