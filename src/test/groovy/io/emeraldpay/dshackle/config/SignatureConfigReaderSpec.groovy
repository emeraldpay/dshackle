package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.test.TestingCommons
import org.bouncycastle.util.io.pem.PemObject
import org.bouncycastle.util.io.pem.PemWriter
import spock.lang.Specification

import java.security.KeyPairGenerator
import java.security.SecureRandom
import java.security.spec.ECGenParameterSpec
import java.security.spec.PKCS8EncodedKeySpec

class SignatureConfigReaderSpec extends Specification {

    def "Parse enabled"() {
        setup:
        def config = "signed-response:\n" +
                "  enabled: true\n" +
                "  algorithm: SECP256K1\n" +
                "  private-key: /root/key.pem\n"

        when:
        def reader = new SignatureConfigReader(TestingCommons.fileResolver())
        def act = reader.read(new ByteArrayInputStream(config.bytes))

        then:
        act.enabled
        act.privateKey == "/root/key.pem"
        act.algorithm == SignatureConfig.Algorithm.SECP256K1
    }

    def "No path when disabled"() {
        setup:
        def config = "signed-response:\n" +
                "  enabled: false\n" +
                "  private-key: /root/key.pem\n"

        when:
        def reader = new SignatureConfigReader(TestingCommons.fileResolver())
        def act = reader.read(new ByteArrayInputStream(config.bytes))

        then:
        !act.enabled
        act.privateKey == null
    }

}
