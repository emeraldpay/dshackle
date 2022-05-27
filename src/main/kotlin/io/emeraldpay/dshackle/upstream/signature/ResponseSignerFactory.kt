package io.emeraldpay.dshackle.upstream.signature

import io.emeraldpay.dshackle.config.SignatureConfig
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyFactory
import java.security.MessageDigest
import java.security.PrivateKey
import java.security.PublicKey
import java.security.interfaces.ECPrivateKey
import java.security.spec.PKCS8EncodedKeySpec
import org.apache.commons.codec.binary.Hex
import org.bouncycastle.jce.ECNamedCurveTable
import org.bouncycastle.math.ec.ECPoint
import org.bouncycastle.util.io.pem.PemObject
import org.bouncycastle.util.io.pem.PemReader
import org.bouncycastle.jce.spec.ECPublicKeySpec
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository

@Repository
open class ResponseSignerFactory(
    @Autowired private val config: SignatureConfig
) : FactoryBean<ResponseSigner> {

    companion object {
        private val log = LoggerFactory.getLogger(ResponseSignerFactory::class.java)
    }

    fun readKey(algorithm: SignatureConfig.Algorithm, keyPath: String): Pair<ECPrivateKey, Long> {
        val reader = PemReader(Files.newBufferedReader(Path.of(keyPath)))
        return readKey(algorithm, reader.readPemObject())
    }

    private fun readKey(algorithm: SignatureConfig.Algorithm, pem: PemObject): Pair<ECPrivateKey, Long> {
        val keyFactory = KeyFactory.getInstance("EC")
        val key = when (algorithm) {
            SignatureConfig.Algorithm.SECP256K1 -> {
                val keySpec = PKCS8EncodedKeySpec(pem.content)
                keyFactory.generatePrivate(keySpec)
            }
        }

        if (key !is ECPrivateKey) {
            throw IllegalStateException("Only ECDSA SECP256K1 keys are allowed")
        }

        if (key.params.toString() != "secp256k1 (1.3.132.0.10)") {
            throw IllegalStateException("Only SECP256K1 are allowed for signing a response")
        }

        val publicKey = extractPublicKey(keyFactory, key)
        val id = getPublicKeyId(publicKey)

        return Pair(key, id)
    }

    fun extractPublicKey(keyFactory: KeyFactory, privateKey: ECPrivateKey): PublicKey {
        val ecSpec = ECNamedCurveTable.getParameterSpec("secp256k1")
        val q: ECPoint = ecSpec.g.multiply(privateKey.s)
        return keyFactory.generatePublic(ECPublicKeySpec(q, ecSpec))
    }

    private fun getPublicKeyId(publicKey: PublicKey): Long {
        val digest = MessageDigest.getInstance("SHA-256")
        val fullId = digest.digest(publicKey.encoded)
        log.info("Using key to sign responses: ${Hex.encodeHexString(fullId).substring(0..15)}")
        return ByteBuffer.wrap(fullId).asLongBuffer().get()
    }

    override fun getObject(): ResponseSigner {
        if (!config.enabled) {
            return NoSigner()
        }
        if (config.privateKey == null) {
            log.warn("Private Key for response signature is not set")
            return NoSigner()
        }
        val key = readKey(config.algorithm, config.privateKey!!)
        return Secp256KSigner(key.first, key.second)
    }

    override fun getObjectType(): Class<*>? {
        return ResponseSigner::class.java
    }

}