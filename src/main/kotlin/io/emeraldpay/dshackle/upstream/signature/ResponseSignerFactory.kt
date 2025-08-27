package io.emeraldpay.dshackle.upstream.signature

import io.emeraldpay.dshackle.config.SignatureConfig
import org.apache.commons.codec.binary.Hex
import org.bouncycastle.jce.ECNamedCurveTable
import org.bouncycastle.jce.spec.ECPublicKeySpec
import org.bouncycastle.math.ec.ECPoint
import org.bouncycastle.util.io.pem.PemObject
import org.bouncycastle.util.io.pem.PemReader
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Lazy
import org.springframework.stereotype.Service
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyFactory
import java.security.MessageDigest
import java.security.PublicKey
import java.security.interfaces.ECPrivateKey
import java.security.spec.PKCS8EncodedKeySpec

@Service @Lazy
open class ResponseSignerFactory(
    @Autowired private val signatureConfig: SignatureConfig,
) : FactoryBean<ResponseSigner> {
    companion object {
        private val log = LoggerFactory.getLogger(ResponseSignerFactory::class.java)
    }

    fun readKey(
        algorithm: SignatureConfig.Algorithm,
        keyPath: String,
    ): Pair<ECPrivateKey, Long> {
        val reader = PemReader(Files.newBufferedReader(Path.of(keyPath)))
        return readKey(algorithm, reader.readPemObject())
    }

    private fun readKey(
        algorithm: SignatureConfig.Algorithm,
        pem: PemObject,
    ): Pair<ECPrivateKey, Long> {
        val keyFactory = KeyFactory.getInstance("EC")
        val key =
            when (algorithm) {
                SignatureConfig.Algorithm.SECP256K1, SignatureConfig.Algorithm.NIST_P256 -> {
                    val keySpec = PKCS8EncodedKeySpec(pem.content)
                    keyFactory.generatePrivate(keySpec)
                }
            }

        if (key !is ECPrivateKey) {
            throw IllegalStateException("Only EC keys are allowed")
        }

        if (algorithm == SignatureConfig.Algorithm.SECP256K1 &&
            key.params.toString().indexOf(SignatureConfig.Algorithm.SECP256K1.getCurveName()) < 0
        ) {
            throw IllegalStateException("Key is not SECP256K1, generate SECP256K1 or use another algorithm")
        }

        if (algorithm == SignatureConfig.Algorithm.NIST_P256 &&
            key.params.toString().indexOf(SignatureConfig.Algorithm.NIST_P256.getCurveName()) < 0
        ) {
            throw IllegalStateException("Key is not NIST P256, generate NIST P256 or use another algorithm")
        }

        val publicKey = extractPublicKey(keyFactory, key, algorithm)
        val id = getPublicKeyId(publicKey)

        return Pair(key, id)
    }

    fun extractPublicKey(
        keyFactory: KeyFactory,
        privateKey: ECPrivateKey,
        algorithm: SignatureConfig.Algorithm,
    ): PublicKey {
        val ecSpec = ECNamedCurveTable.getParameterSpec(algorithm.getCurveName())
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
        if (!signatureConfig.enabled) {
            return NoSigner()
        }
        if (signatureConfig.privateKey == null) {
            log.warn("Private Key for response signature is not set")
            return NoSigner()
        }
        val key = readKey(signatureConfig.algorithm, signatureConfig.privateKey!!)
        return EcdsaSigner(key.first, key.second)
    }

    override fun getObjectType(): Class<*>? = ResponseSigner::class.java
}
