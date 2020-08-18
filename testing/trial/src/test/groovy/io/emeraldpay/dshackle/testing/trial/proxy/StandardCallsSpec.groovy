package io.emeraldpay.dshackle.testing.trial.proxy

import io.emeraldpay.dshackle.testing.trial.ProxyClient
import spock.lang.Specification

class StandardCallsSpec extends Specification {

    def client = ProxyClient.forPrefix("eth")

    def "get height"() {
        when:
        def act = client.execute("eth_blockNumber", [])
        then:
        act.result == "0x100001"
        act.error == null
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
    }
}
