package io.emeraldpay.dshackle.rpc

import io.emeraldpay.dshackle.config.CacheConfig
import io.emeraldpay.dshackle.config.SignatureConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.security.Signature
import javax.annotation.PostConstruct

@Repository
open class CurrentResponseSigner(
    @Autowired private val config: SignatureConfig?
) {
    fun sign(message: ByteArray): String {
        if (config == null || !config.enabled) {
            return ""
        }
        val sig = Signature.getInstance(config.signScheme)
        if (config.privateKey == null) {
            throw Exception("Signatures are enabled, but private key is not configured")
        }
        sig.initSign(config.privateKey)
        sig.update(message)
        return sig.sign().toString()
    }
}