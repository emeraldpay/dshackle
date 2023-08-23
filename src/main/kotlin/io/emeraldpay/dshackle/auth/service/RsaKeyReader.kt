package io.emeraldpay.dshackle.auth.service

import org.bouncycastle.openssl.PEMParser
import org.springframework.stereotype.Component
import java.io.StringReader
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyFactory
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec

@Component
class RsaKeyReader : KeyReader {
    private val factory = KeyFactory.getInstance("RSA")

    override fun getKeyPair(providerPrivateKeyPath: String, externalPublicKeyPath: String): KeyReader.Keys {
        val privateKeyReader = StringReader(Files.readString(Paths.get(providerPrivateKeyPath)))
        val publicKeyReader = StringReader(Files.readString(Paths.get(externalPublicKeyPath)))

        val privatePem = PEMParser(privateKeyReader).readPemObject()
        val publicPem = PEMParser(publicKeyReader).readPemObject()

        val privateKeySpec = PKCS8EncodedKeySpec(privatePem.content)
        val publicKeySpec = X509EncodedKeySpec(publicPem.content)

        val pubKey = factory.generatePublic(publicKeySpec)
        val privateKey = factory.generatePrivate(privateKeySpec)

        return KeyReader.Keys(
            privateKey,
            pubKey
        )
    }
}
