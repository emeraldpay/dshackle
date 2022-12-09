package io.emeraldpay.dshackle.upstream.signature

import io.emeraldpay.dshackle.config.SignatureConfig
import io.emeraldpay.dshackle.upstream.Upstream
import org.apache.commons.codec.binary.Hex
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.io.pem.PemObject
import org.bouncycastle.util.io.pem.PemWriter
import spock.lang.Specification

import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.MessageDigest
import java.security.Security
import java.security.Signature
import java.security.interfaces.ECPrivateKey
import java.security.spec.ECGenParameterSpec
import java.security.spec.PKCS8EncodedKeySpec

class EcdsaSignerSpec extends Specification {

    def setupSpec() {
        Security.addProvider(new BouncyCastleProvider())
    }

    def "Reads private key NIST P256"() {
        setup:
        def file = File.createTempFile("test", ".pem")
        def keygen = KeyPairGenerator.getInstance("EC")
        keygen.initialize(new ECGenParameterSpec("secp256r1"))
        def key = keygen.generateKeyPair()
        def keyBuilder = new PKCS8EncodedKeySpec(key.getPrivate().getEncoded())
        def writer = new PemWriter(new FileWriter(file.path))
        writer.writeObject(new PemObject("PRIVATE KEY", keyBuilder.getEncoded()))
        writer.close()

        when:
        def signer = new ResponseSignerFactory(new SignatureConfig())
        def act = signer.readKey(SignatureConfig.Algorithm.NIST_P256, file.absolutePath).first

        then:
        act == key.getPrivate()

        cleanup:
        file.delete()
    }

    def "Id is a hash of x509 public key"() {
        setup:
        def conf = new SignatureConfig()
        conf.enabled = true
        conf.privateKey = "testing/dshackle/test_key"
        def signer = new ResponseSignerFactory(conf).getObject() as EcdsaSigner

        // To verify the test, check the hash of test key above:
        //
        // echo MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAE3zetdMdyTO/sTFCLeOrI5moiZt2RjfUVdavhorgqd+gxAqM01cf5Q4QZ8INne9RykcQsbLYXQfDXJbGMm5+gdg== | base64 -d - | shasum -a 256
        // d25f1ff2c1a57235a9bc7725cd645ab0e9631475a12402f2881579d3f6887597  -
        //

        when:
        def id = signer.keyId

        then:
        id == 0xed397068b172b393L
    }

    def "Wrap message"() {
        setup:
        def up = Mock(Upstream) {
            _ * getId() >> "infura"
        }
        def signer = new EcdsaSigner(Stub(ECPrivateKey), 100L)

        when:
        def act = signer.wrapMessage(10, "test".bytes, up.id)

        then:
        act == "DSHACKLESIG/10/infura/9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    }

    def "Signed message is valid"() {
        setup:
        def result = "test".bytes
        def up = Mock(Upstream) {
            _ * getId() >> "infura"
        }

        def keyPairGen = KeyPairGenerator.getInstance("EC")
        keyPairGen.initialize(new ECGenParameterSpec("secp256r1"))
        def pair = keyPairGen.generateKeyPair()
        def verifier = Signature.getInstance("SHA256withECDSA")
        verifier.initVerify(pair.getPublic())
        verifier.update("DSHACKLESIG/10/infura/9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08".getBytes())

        def signer = new EcdsaSigner((pair.getPrivate() as ECPrivateKey), 100L)

        when:
        def sig = signer.sign(10, result, up.id)

        then:
        verifier.verify(sig.value)
    }

    def "Signed message is valid - for docs"() {
        // it's the example used in docs
        setup:
        def result = '["0xe670ec64341771606e55d6b4ca35a1a6b75ee3d5145a99d05921026d1527331", true]'.bytes
        def up = Mock(Upstream) {
            _ * getId() >> "infura"
        }

        def sha256 = MessageDigest.getInstance("SHA-256")

        def conf = new SignatureConfig()
        conf.enabled = true
        conf.privateKey = "testing/dshackle/test_key"
        def factory = new ResponseSignerFactory(conf)

        def sk = factory.readKey(conf.algorithm, conf.privateKey).first
        def pk = factory.extractPublicKey(KeyFactory.getInstance("EC"), sk, SignatureConfig.Algorithm.NIST_P256)
        def verifier = Signature.getInstance("SHA256withECDSA")
        verifier.initVerify(pk)
        verifier.update("DSHACKLESIG/10/infura/${Hex.encodeHexString(sha256.digest(result))}".getBytes())

        def signer = factory.getObject() as EcdsaSigner

        when:
        def sig = signer.sign(10, result, up.id)
        println("Signature: ${Hex.encodeHexString(sig.value)}")

        then:
        verifier.verify(sig.value)
    }
}
