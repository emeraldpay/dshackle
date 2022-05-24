package io.emeraldpay.dshackle.rpc

import com.google.common.primitives.Bytes
import com.google.common.primitives.Longs
import io.emeraldpay.dshackle.config.SignatureConfig
import org.apache.commons.codec.binary.Hex
import spock.lang.Specification

import java.security.KeyPairGenerator
import java.security.SecureRandom
import java.security.Signature
import java.security.spec.ECGenParameterSpec

class CurrentResponseSignerSpec extends Specification {
    def "Does nothing if not enabled"() {
        setup:
        def conf = new SignatureConfig()
        def signer = new CurrentResponseSigner(conf)
        when:
        def sig = signer.sign(10, "test".bytes)
        then:
        sig == null
    }

    def "Throw exception if privkey not configured"() {
        setup:
        def conf = new SignatureConfig()
        conf.enabled = true
        def signer = new CurrentResponseSigner(conf)
        when:
        def sig = signer.sign(10, "test".bytes)
        then:
        def exception= thrown(Exception)
        exception.message.indexOf("private key is not configured") != -1
    }

    def "Signs message"() {
        setup:
        def conf = new SignatureConfig()
        conf.enabled = true
        def keyPairGen = KeyPairGenerator.getInstance(conf.algorithmAsString())
        def ecGenParameterSpec = new ECGenParameterSpec("secp256k1");
        keyPairGen.initialize(ecGenParameterSpec, new SecureRandom())
        def pair = keyPairGen.generateKeyPair()
        conf.privateKey = pair.getPrivate()
        def signer = new CurrentResponseSigner(conf)
        def sign = Signature.getInstance(CurrentResponseSigner.SIGN_SCHEME)
        sign.initVerify(pair.getPublic())
        sign.update(Bytes.concat(Longs.toByteArray(10), "test".bytes))
        when:
        def sig = signer.sign(10, "test".bytes)
        then:
        sign.verify(sig)
    }
}
