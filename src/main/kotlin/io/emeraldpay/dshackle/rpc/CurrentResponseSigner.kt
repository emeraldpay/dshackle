package io.emeraldpay.dshackle.rpc

import com.google.common.primitives.Longs
import io.emeraldpay.dshackle.config.SignatureConfig
import org.apache.commons.codec.binary.Hex
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.security.Signature

@Repository
open class CurrentResponseSigner(
    @Autowired private val config: SignatureConfig
) : ResponseSigner {
    override fun sign(nonce: Long, message: ByteArray): ByteArray? {
        if (!config.enabled) {
            return null
        }
        val sig = Signature.getInstance(config.signSchemeAsString())
        if (config.privateKey == null) {
            throw Exception("Signatures are enabled, but private key is not configured")
        }
        sig.initSign(config.privateKey)
        sig.update(Longs.toByteArray(nonce) + message)
        return sig.sign()
    }
}