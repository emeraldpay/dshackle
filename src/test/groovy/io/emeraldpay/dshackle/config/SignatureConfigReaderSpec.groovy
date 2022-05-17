package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.test.TestingCommons
import org.bouncycastle.util.io.pem.PemObject
import org.bouncycastle.util.io.pem.PemWriter
import org.yaml.snakeyaml.nodes.MappingNode
import spock.lang.Specification

import java.security.KeyPairGenerator
import java.security.spec.PKCS8EncodedKeySpec

class SignatureConfigReaderSpec extends Specification {
    def "Reads signature config"() {
        def file = File.createTempFile("test", ".key")
        def keygen = KeyPairGenerator.getInstance("RSA")
        keygen.initialize(2048)
        def key = keygen.generateKeyPair()
        def keyBuilder = new PKCS8EncodedKeySpec(key.getPrivate().getEncoded())
        def writer = new PemWriter(new FileWriter(file.path))
        writer.writeObject(new PemObject("PRIVATE KEY", keyBuilder.getEncoded()))
        writer.flush()
        def config = "signature:\n" +
                "  enabled: true\n" +
                "  scheme: SHA256withRSA\n" +
                "  algorithm: RSA\n" +
                "  privateKey: " + file.path
        when:
        def reader = new SignatureConfigReader(TestingCommons.fileResolver())
        def resConfig = reader.read(new ByteArrayInputStream(config.bytes))
        then:
        resConfig.signScheme == "SHA256withRSA"
        resConfig.algorithm == "RSA"
        resConfig.enabled
        resConfig.privateKey == key.getPrivate()
        file.delete()
    }
}
