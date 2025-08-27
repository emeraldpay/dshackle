package io.emeraldpay.dshackle.config

import java.util.Locale

class SignatureConfig {
    enum class Algorithm {
        SECP256K1,
        NIST_P256,
        ;

        fun getCurveName(): String =
            if (this == SECP256K1) {
                "secp256k1"
            } else if (this == NIST_P256) {
                "secp256r1"
            } else {
                throw IllegalStateException()
            }
    }

    companion object {
        fun algorithmOfString(algo: String): Algorithm {
            val algorithm =
                when (algo.uppercase(Locale.getDefault())) {
                    "SECP256K1" -> Algorithm.SECP256K1
                    "NIST_P256", "NIST-P256", "NISTP256", "SECP256R1" -> Algorithm.NIST_P256
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
