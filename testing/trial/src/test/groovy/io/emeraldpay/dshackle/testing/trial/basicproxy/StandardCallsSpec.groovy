package io.emeraldpay.dshackle.testing.trial.basicproxy

import io.emeraldpay.dshackle.testing.trial.ProtoClient
import io.emeraldpay.dshackle.testing.trial.ProxyClient
import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Specification

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
        def act = client_proto.executeNative("eth_blockNumber", [], "test")
        then:
        act.signature == "302e0215009a561b8a8aaa5a4a7c12b672f92da15aacf51940021500ad3ce9f2e20053ceff652130e07674cb3843e6b8"
    }

    def "check response signature without nonce"() {
        when:
        def act = client_proto.executeNative("eth_blockNumber", [], "")
        then:
        act.signature == ""
    }
}
