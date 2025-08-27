package io.emeraldpay.dshackle.upstream.signature

import io.emeraldpay.dshackle.upstream.Upstream

interface ResponseSigner {
    fun sign(
        nonce: Long,
        message: ByteArray,
        source: Upstream,
    ): Signature?

    data class Signature(
        val value: ByteArray,
        val upstreamId: String,
        val keyId: Long,
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Signature) return false

            if (!value.contentEquals(other.value)) return false
            if (upstreamId != other.upstreamId) return false
            if (keyId != other.keyId) return false

            return true
        }

        override fun hashCode(): Int {
            var result = value.contentHashCode()
            result = 31 * result + upstreamId.hashCode()
            result = 31 * result + keyId.hashCode()
            return result
        }
    }
}
