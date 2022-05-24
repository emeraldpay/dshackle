package io.emeraldpay.dshackle.config

import java.security.interfaces.ECPrivateKey

class SignatureConfig {
    enum class Algorithm {
        ECDSA
    }

    class UnknownAlgorithm(message:String): Exception(message)

    companion object {
        fun algorithmOfString(algo: String): Algorithm {
            val algorithm = when (algo) {
                "ECDSA" -> Algorithm.ECDSA
                else -> throw UnknownAlgorithm("Unknow algorithm or not allowed")
            }
            return algorithm
        }
    }

    /**
     * Signature scheme that we should use
     */
    var algorithm: Algorithm = Algorithm.ECDSA
    /**
     * Should we generate signature on this instance if it's not already present
     */
    var enabled: Boolean = false

    var privateKey : ECPrivateKey? = null

    fun algorithmAsString(): String {
        val scheme = when (algorithm) {
            Algorithm.ECDSA -> "EC"
        }
        return scheme
    }
}