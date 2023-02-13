package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.test.TestingCommons
import spock.lang.Specification

class SignatureConfigReaderSpec extends Specification {

    def "Parse enabled"() {
        setup:
        def config = "signed-response:\n" +
                "  enabled: true\n" +
                "  algorithm: NIST_P256\n" +
                "  private-key: /root/key.pem\n"

        when:
        def reader = new SignatureConfigReader(TestingCommons.fileResolver())
        def act = reader.read(new ByteArrayInputStream(config.bytes))

        then:
        act.enabled
        act.privateKey == "/root/key.pem"
        act.algorithm == SignatureConfig.Algorithm.NIST_P256
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
