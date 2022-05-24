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
    companion object Constants {
        val SIGN_SCHEME = "SHA256withECDSA"
        val SIGN_PREFIX = "DSHACKLESIG".toByteArray()
    }
    override fun sign(nonce: Long, message: ByteArray): ByteArray? {
        if (!config.enabled) {
            return null
        }
        val sig = Signature.getInstance(SIGN_SCHEME)
        if (config.privateKey == null) {
            throw Exception("Signatures are enabled, but private key is not configured")
        }
        sig.initSign(config.privateKey)
        // We add prefix to avoid the attack when data provider and client collude and are able to sign arbitrary messages
        // If instance private key holds some value, it could be dangerous
        sig.update(SIGN_PREFIX + Longs.toByteArray(nonce) + message)
        return sig.sign()
    }
}