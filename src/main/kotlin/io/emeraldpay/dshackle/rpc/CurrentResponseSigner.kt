package io.emeraldpay.dshackle.rpc

import io.emeraldpay.dshackle.config.SignatureConfig
import org.apache.commons.codec.binary.Hex
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.security.Signature

@Repository
open class CurrentResponseSigner(
    @Autowired private val config: SignatureConfig
) : ResponseSigner {
    override fun sign(message: ByteArray): String {
        if (!config.enabled) {
            return ""
        }
        val sig = Signature.getInstance(SignatureConfig.sigSchemeToString(config.signScheme))
        if (config.privateKey == null) {
            throw Exception("Signatures are enabled, but private key is not configured")
        }
        sig.initSign(config.privateKey)
        sig.update(message)
        return Hex.encodeHexString(sig.sign())
    }
}