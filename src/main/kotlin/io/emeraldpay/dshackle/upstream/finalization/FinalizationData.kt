package io.emeraldpay.dshackle.upstream.finalization

import io.emeraldpay.api.proto.Common

class FinalizationData(
    val height: Long,
    val type: FinalizationType,
) {

    override fun toString(): String {
        return "FinalizationData($height, ${type.toBlockRef()})"
    }
    override fun equals(other: Any?): Boolean {
        return when (other) {
            is FinalizationData -> other.height == height && other.type == type
            else -> false
        }
    }
}

enum class FinalizationType {
    UNKNOWN,
    SAFE_BLOCK,
    FINALIZED_BLOCK,
    ;

    companion object {
        fun fromBlockRef(v: String): FinalizationType {
            return when (v) {
                "safe" -> SAFE_BLOCK
                "finalized" -> FINALIZED_BLOCK
                else -> UNKNOWN
            }
        }
    }

    fun toProtoFinalizationType(): Common.FinalizationType {
        return when (this) {
            FINALIZED_BLOCK -> Common.FinalizationType.FINALIZATION_FINALIZED_BLOCK
            UNKNOWN -> Common.FinalizationType.UNRECOGNIZED
            SAFE_BLOCK -> Common.FinalizationType.FINALIZATION_SAFE_BLOCK
        }
    }

    fun toBlockRef(): String {
        return when (this) {
            FINALIZED_BLOCK -> "finalized"
            SAFE_BLOCK -> "safe"
            UNKNOWN -> "unknown"
        }
    }
}

fun Common.FinalizationType.fromProtoType(): FinalizationType {
    return when (this) {
        Common.FinalizationType.FINALIZATION_UNSPECIFIED -> FinalizationType.UNKNOWN
        Common.FinalizationType.FINALIZATION_SAFE_BLOCK -> FinalizationType.SAFE_BLOCK
        Common.FinalizationType.FINALIZATION_FINALIZED_BLOCK -> FinalizationType.FINALIZED_BLOCK
        Common.FinalizationType.UNRECOGNIZED -> FinalizationType.UNKNOWN
    }
}
