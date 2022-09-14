package io.emeraldpay.dshackle.upstream.signature

class NoSigner : ResponseSigner {
    override fun sign(nonce: Long, message: ByteArray, source: String): ResponseSigner.Signature? {
        return null
    }
}
