package io.emeraldpay.dshackle.rpc

import io.emeraldpay.dshackle.config.SignatureConfig
import org.apache.commons.codec.binary.Hex
import spock.lang.Specification

import java.security.KeyPairGenerator
import java.security.Signature

class CurrentResponseSignerSpec extends Specification {
    def "Does nothing if not enabled"() {
        setup:
        def conf = new SignatureConfig()
        def signer = new CurrentResponseSigner(conf)
        when:
        def sig = signer.sign("test".bytes)
        then:
        sig == ""
    }

    def "Throw exception if privkey not configured"() {
        setup:
        def conf = new SignatureConfig()
        conf.enabled = true
        def signer = new CurrentResponseSigner(conf)
        when:
        def sig = signer.sign("test".bytes)
        then:
        def exception= thrown(Exception)
        exception.message.indexOf("private key is not configured") != -1
    }

    def "Signs message"() {
        setup:
        def conf = new SignatureConfig()
        conf.enabled = true
        def keyPairGen = KeyPairGenerator.getInstance(conf.algorithm)
        keyPairGen.initialize(2048)
        def pair = keyPairGen.generateKeyPair()
        conf.privateKey = pair.getPrivate()
        def signer = new CurrentResponseSigner(conf)
        def sign = Signature.getInstance(conf.signScheme)
        sign.initVerify(pair.getPublic())
        sign.update("test".bytes)
        when:
        def sig = signer.sign("test".bytes)
        then:
        sign.verify(Hex.decodeHex(sig))
    }
}
