package io.emeraldpay.dshackle.rpc

interface ResponseSigner {
    fun sign(message: ByteArray): String
}