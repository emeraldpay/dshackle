package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.test.TestingCommons
import org.bouncycastle.util.io.pem.PemObject
import org.bouncycastle.util.io.pem.PemWriter
import org.yaml.snakeyaml.nodes.MappingNode
import spock.lang.Specification

import java.security.KeyPairGenerator
import java.security.SecureRandom
import java.security.spec.ECGenParameterSpec
import java.security.spec.PKCS8EncodedKeySpec

class SignatureConfigReaderSpec extends Specification {
    def "Reads signature config"() {
        def file = File.createTempFile("test", ".key")
        Random rand = new SecureRandom()
        def keygen = KeyPairGenerator.getInstance("EC")
            ECGenParameterSpec ecGenParameterSpec = new ECGenParameterSpec("secp256k1");
        keygen.initialize(ecGenParameterSpec, rand)
        def key = keygen.generateKeyPair()
        def keyBuilder = new PKCS8EncodedKeySpec(key.getPrivate().getEncoded())
        def writer = new PemWriter(new FileWriter(file.path))
        writer.writeObject(new PemObject("PRIVATE KEY", keyBuilder.getEncoded()))
        writer.flush()
        def config = "signature:\n" +
                "  enabled: true\n" +
                "  algorithm: ECDSA\n" +
                "  privateKey: " + file.path
        when:
        def reader = new SignatureConfigReader(TestingCommons.fileResolver())
        def resConfig = reader.read(new ByteArrayInputStream(config.bytes))
        then:
        resConfig.algorithm == SignatureConfig.Algorithm.ECDSA
        resConfig.enabled
        resConfig.privateKey == key.getPrivate()
        file.delete()
    }

    def "Fails to read restricted key type"() {
        def config = "signature:\n" +
                "  enabled: true\n" +
                "  algorithm: RSA\n"
        when:
        def reader = new SignatureConfigReader(TestingCommons.fileResolver())
        def resConfig = reader.read(new ByteArrayInputStream(config.bytes))
        then:
        thrown SignatureConfig.UnknownAlgorithm
    }

    def "Fails to read restricted non-secp256k1 key"() {
        def file = File.createTempFile("test", ".key")
        Random rand = new SecureRandom()
        def keygen = KeyPairGenerator.getInstance("EC")
        ECGenParameterSpec ecGenParameterSpec = new ECGenParameterSpec("secp256r1");
        keygen.initialize(ecGenParameterSpec, rand)
        def key = keygen.generateKeyPair()
        def keyBuilder = new PKCS8EncodedKeySpec(key.getPrivate().getEncoded())
        def writer = new PemWriter(new FileWriter(file.path))
        writer.writeObject(new PemObject("PRIVATE KEY", keyBuilder.getEncoded()))
        writer.flush()
        def config = "signature:\n" +
                "  enabled: true\n" +
                "  algorithm: ECDSA\n" +
                "  privateKey: " + file.path
        when:
        def reader = new SignatureConfigReader(TestingCommons.fileResolver())
        def resConfig = reader.read(new ByteArrayInputStream(config.bytes))
        then:
        thrown SignatureConfigReader.SignatureCurveError
    }
}
