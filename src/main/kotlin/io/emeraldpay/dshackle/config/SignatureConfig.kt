package io.emeraldpay.dshackle.config

import org.bouncycastle.jcajce.provider.asymmetric.ec.KeyFactorySpi.ECDSA
import java.security.PrivateKey
class SignatureConfig {
    enum class Algorithm {
        ECDSA
    }

    enum class SigScheme {
        SHA256withECDSA, SHA3256withECDSA,
        SHA3384withECDSA, SHA3512withECDSA,
        SHA512withECDSA
    }

    class UnknownSignature(message:String): Exception(message)
    class UnknownAlgorithm(message:String): Exception(message)

    companion object {

        fun sigSchemeOfString(sigscheme: String): SigScheme {
            val scheme = when (sigscheme) {
                "SHA256withECDSA" -> SigScheme.SHA256withECDSA
                "SHA3-256withECDSA" -> SigScheme.SHA3256withECDSA
                "SHA3-384withECDSA" -> SigScheme.SHA3384withECDSA
                "SHA3-512withECDSA" -> SigScheme.SHA3512withECDSA
                "SHA512withECDSA" -> SigScheme.SHA512withECDSA
                else -> throw UnknownSignature("Unknown signature scheme or not allowed")
            }
            return scheme
        }
        fun sigSchemeToString(sigscheme: SigScheme): String {
            val scheme = when (sigscheme) {
                SigScheme.SHA256withECDSA -> "SHA256withECDSA"
                SigScheme.SHA3256withECDSA -> "SHA3-256withECDSA"
                SigScheme.SHA3384withECDSA -> "SHA3-384withECDSA"
                SigScheme.SHA3512withECDSA -> "SHA3-512withECDSA"
                SigScheme.SHA512withECDSA -> "SHA512withECDSA"
            }
            return scheme
        }

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
    var signScheme: SigScheme = SigScheme.SHA256withECDSA
    var algorithm: Algorithm = Algorithm.ECDSA
    /**
     * Should we generate signature on this instance if it's not already present
     */
    var enabled: Boolean = false

    var privateKey : PrivateKey? = null

}