package io.emeraldpay.dshackle.testing.trial.proxy

import io.emeraldpay.dshackle.testing.trial.ProxyClient
import spock.lang.Specification

class GivesErrorSpec extends Specification {

    def client = ProxyClient.forPrefix("eth")

    def "Gives error message"() {
        // issue #42
        when:
        def act = client.execute("eth_call", [[to: "0x542156d51D10Db5acCB99f9Db7e7C91B74E80a2c"]])
        then:
        act.result == null
        act.error != null
        with(act.error) {
            code == -32015
            message == "VM execution error."
        }
    }

    def "Gives error data"() {
        // issue #42
        when:
        def act = client.execute("eth_call", [[to: "0x8ee2a5aca4f88cb8c757b8593d0734855dcc0eba"]])
        then:
        act.result == null
        act.error != null
        with(act.error) {
            code == -32015
            message == "VM execution error."
            data == "revert: SafeMath: division by zero"
        }
    }

    def "Dispatch error from upstream when block is specified"() {
        // issue #67
        when:
        def call = [
                to  : "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                from: "0xEF65ffB384c99a00403EAa22115323a555700D79",
                data: "0xa9059cbb0000000000000000000000003f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be000000000000000000000000000000000000000000000000000000004856fb60"
        ]
        def act = client.execute("eth_call", [call, "0x100000"])
        then:
        act.result == null
        act.error != null
        with(act.error) {
            code == -32000
            message == "invalid opcode: opcode 0xfe not defined"
        }
    }

    def "Dispatch error from upstream when custome method is used"() {
        // issue #67
        when:
        def call = [
                to  : "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                from: "0xEF65ffB384c99a00403EAa22115323a555700D79",
                data: "0xa9059cbb0000000000000000000000003f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be000000000000000000000000000000000000000000000000000000004856fb60"
        ]
        def act = client.execute("test_foo", [call, "0x100000"])
        then:
        act.result == null
        act.error != null
        with(act.error) {
            code == -32000
            message == "invalid opcode: opcode 0xfe not defined"
        }
    }
}
