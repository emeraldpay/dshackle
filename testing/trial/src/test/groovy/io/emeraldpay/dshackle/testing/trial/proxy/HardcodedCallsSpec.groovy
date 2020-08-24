package io.emeraldpay.dshackle.testing.trial.proxy

import io.emeraldpay.dshackle.testing.trial.ProxyClient
import spock.lang.Specification

class HardcodedCallsSpec extends Specification {

    def client = ProxyClient.forPrefix("eth")

    def "get syncing"() {
        when:
        def act = client.execute("eth_syncing", [])
        then:
        act.result == false
        act.error == null
    }

    def "get network version"() {
        when:
        def act = client.execute("net_version", [])
        then:
        act.result == "1"
        act.error == null
    }

    def "get peer count"() {
        when:
        def act = client.execute("net_peerCount", [])
        then:
        act.result == "0x2a"
        act.error == null
    }

}
