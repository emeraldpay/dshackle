package io.emeraldpay.dshackle.config

import org.bouncycastle.jcajce.provider.asymmetric.ec.KeyFactorySpi.ECDSA
import java.security.PrivateKey
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

    var privateKey : PrivateKey? = null

    fun algorithmAsString(): String {
        val scheme = when (algorithm) {
            Algorithm.ECDSA -> "EC"
        }
        return scheme
    }
}