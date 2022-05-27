package io.emeraldpay.dshackle.config

import java.util.Locale

class SignatureConfig {

    enum class Algorithm {
        SECP256K1
    }

    companion object {
        fun algorithmOfString(algo: String): Algorithm {
            val algorithm = when (algo.uppercase(Locale.getDefault())) {
                "SECP256K1" -> Algorithm.SECP256K1
                else -> throw IllegalArgumentException("Unknown algorithm or not allowed")
            }
            return algorithm
        }
    }

    /**
     * Signature scheme that we should use
     */
    var algorithm: Algorithm = Algorithm.SECP256K1
    /**
     * Should we generate signature on this instance if it's not already present
     */
    var enabled: Boolean = false

    var privateKey: String? = null
}
