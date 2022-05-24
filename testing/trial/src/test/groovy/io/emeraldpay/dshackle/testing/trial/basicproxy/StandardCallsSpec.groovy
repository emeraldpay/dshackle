package io.emeraldpay.dshackle.testing.trial.basicproxy

import com.google.common.primitives.Bytes
import com.google.common.primitives.Longs
import io.emeraldpay.dshackle.testing.trial.ProtoClient
import io.emeraldpay.dshackle.testing.trial.ProxyClient
import org.apache.commons.codec.binary.Hex
import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Specification
import java.security.KeyFactory
import org.bouncycastle.util.io.pem.PemReader

import java.security.MessageDigest
import java.security.Signature
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec

@IgnoreIf({ System.getProperty('trialMode') != 'basic' })
class StandardCallsSpec extends Specification {

    @Shared client_proto = ProtoClient.basic()
    @Shared client_proxy = ProxyClient.forPrefix("eth")

    @Shared clients = [client_proto, client_proxy]

    def "get height"() {
        when:
        def act = client.execute("eth_blockNumber", [])
        then:
        act.result == "0x100001"
        act.error == null
        where:
        client << clients
    }

    def "get block"() {
        when:
        def act = client.execute("eth_getBlockByNumber", ["0x100001", false])
        then:
        act.result != null
        with(act.result) {
            number == "0x100001"
            hash == "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446"
            transactions == [
                    "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77",
                    "0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e"
            ]
        }
        act.error == null
        where:
        client << clients
    }

    def "get non-existing block"() {
        when:
        def act = client.execute("eth_getBlockByNumber", ["0x200001", false])
        then:
        act.result == null
        act.error == null
        where:
        client << clients
    }

    def "get tx"() {
        when:
        def act = client.execute("eth_getTransactionByHash", ["0x01c5a8461d06c2c195035c148af0f871c7679841d86ae5bb98676bb2d8e68dfa"])
        then:
        act.result != null
        with(act.result) {
            blockHash == "0x9a834c53bbee9c2665a5a84789a1d1ad73750b2d77b50de44f457f411d02e52e"
        }
        act.error == null
        where:
        client << clients
    }

    def "get non-existing tx"() {
        when:
        def act = client.execute("eth_getTransactionByHash", ["0x000000461d06c2c195035c148af0f871c7679841d86ae5bb98676bb2d8e68dfa"])
        then:
        act.result == null
        act.error == null
        where:
        client << clients
    }

    def "get block with txes"() {
        when:
        def act = client.execute("eth_getBlockByNumber", ["0x100001", true])
        then:
        act.result != null
        with(act.result) {
            number == "0x100001"
            hash == "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446"
            transactions.size() == 2
            transactions[0] instanceof Map
            transactions[1] instanceof Map
            with(transactions.find { it.hash == "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77" }) {
                from == "0x2a65aca4d5fc5b5c859090a6c34d164135398226"
            }
        }
        act.error == null
        where:
        client << clients
    }

    def "returns original block json"() {
        when:
        def act = client.execute("eth_getBlockByNumber", ["0x100001", false])
        then:
        act.result != null
        with(act.result) {
            testFoo == "bar"
        }
        act.error == null
        where:
        client << clients
    }

    def "returns original block json with tx"() {
        when:
        def act = client.execute("eth_getBlockByNumber", ["0x100001", true])
        then:
        act.result != null
        with(act.result) {
            testFoo == "bar"
        }
        act.error == null
        where:
        client << clients
    }

    def "check response signature with nonce"() {
        when:
        def act = client_proto.executeNative("eth_blockNumber", [], 10)
        def keyFactory = KeyFactory.getInstance("EC")
        def key = new File(System.getProperty('signatureKey'))
        def reader = new PemReader(key.newReader())
        def keySpec = new X509EncodedKeySpec(reader.readPemObject().getContent())
        def pubKey = keyFactory.generatePublic(keySpec)
        def sig = Signature.getInstance("SHA256withECDSA")
        sig.initVerify(pubKey)
        def sep = "/".bytes
        def digest = MessageDigest.getInstance("SHA-256")
        def messageHash = digest.digest(act.payload.toByteArray())
        sig.update(Bytes.concat("DSHACKLESIG".bytes, sep, Longs.toByteArray(10), sep, messageHash))
        then:
        (new String(act.payload.toByteArray())) == "\"0x100001\""
        sig.verify(act.signature.sig.toByteArray())
    }

    def "check response signature without nonce"() {
        when:
        def act = client_proto.executeNative("eth_blockNumber", [], 0L)
        then:
        act.signature.sig.isEmpty()
    }
}
