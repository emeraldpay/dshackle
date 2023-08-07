package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.etherjar.hex.HexQuantity

class MaximumValueQuorum : CallQuorum, ValueAwareQuorum<String>(String::class.java) {
    private var max: Long? = null
    private var result: ByteArray? = null
    private var sig: ResponseSigner.Signature? = null

    override fun isResolved(): Boolean {
        return result != null
    }

    override fun isFailed(): Boolean {
        return result == null
    }

    override fun getResult(): ByteArray? {
        return result
    }

    override fun getSignature(): ResponseSigner.Signature? {
        return sig
    }
    override fun recordValue(
        response: ByteArray,
        responseValue: String?,
        signature: ResponseSigner.Signature?,
        upstream: Upstream
    ) {
        val value = responseValue?.let { str ->
            HexQuantity.from(str).value.toLong()
        }
        if (value != null) {
            max = max.let {
                if (it == null || it < value) {
                    sig = signature
                    resolvers.clear()
                    result = response
                    value
                } else {
                    it
                }
            }
        }
    }

    override fun recordError(
        response: ByteArray?,
        errorMessage: String?,
        signature: ResponseSigner.Signature?,
        upstream: Upstream
    ) {
        if (max == null) {
            resolvers.add(upstream)
        }
    }
}
