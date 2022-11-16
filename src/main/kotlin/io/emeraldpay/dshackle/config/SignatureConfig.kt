package io.emeraldpay.dshackle.config

import java.util.Locale

class SignatureConfig {

    enum class Algorithm {
        NIST_P256;

        fun getCurveName(): String {
            return if (this == NIST_P256) {
                "secp256r1"
            } else {
                throw IllegalStateException()
            }
        }
    }

    companion object {
        fun algorithmOfString(algo: String): Algorithm {
            val algorithm = when (algo.uppercase(Locale.getDefault())) {
                "NIST_P256", "NIST-P256", "NISTP256", "SECP256R1" -> Algorithm.NIST_P256
                else -> throw IllegalArgumentException("Unknown algorithm or not allowed")
            }
            return algorithm
        }
    }

    /**
     * Signature scheme that we should use
     */
    var algorithm: Algorithm = Algorithm.NIST_P256
    /**
     * Should we generate signature on this instance if it's not already present
     */
    var enabled: Boolean = false

    var privateKey: String? = null
}
