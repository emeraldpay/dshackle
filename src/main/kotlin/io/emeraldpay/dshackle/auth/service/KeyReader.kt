package io.emeraldpay.dshackle.auth.service

import java.security.PrivateKey
import java.security.PublicKey

interface KeyReader {

    fun getKeyPair(providerPrivateKeyPath: String, externalPublicKeyPath: String): Keys

    data class Keys(
        val providerPrivateKey: PrivateKey,
        val externalPublicKey: PublicKey
    )
}
