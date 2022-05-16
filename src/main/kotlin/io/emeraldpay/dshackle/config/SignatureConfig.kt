package io.emeraldpay.dshackle.config

import java.security.PrivateKey

class SignatureConfig {
    /**
     * Signature scheme that we should use
     */
    var signScheme: String = "SHA256withDSA"
    var algorithm: String = "DSA"
    /**
     * Should we generate signature on this instance if it's not already present
     */
    var enabled: Boolean = false

    var privateKey : PrivateKey? = null

}