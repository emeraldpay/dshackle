package io.emeraldpay.dshackle.rpc

interface ResponseSigner {
    fun sign(nonce: Long, message: ByteArray): ByteArray?

}