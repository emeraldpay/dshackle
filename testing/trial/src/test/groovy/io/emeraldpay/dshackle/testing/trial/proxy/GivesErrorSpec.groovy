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

}
