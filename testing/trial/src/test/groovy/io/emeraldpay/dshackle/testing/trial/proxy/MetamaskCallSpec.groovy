package io.emeraldpay.dshackle.testing.trial.proxy

import io.emeraldpay.dshackle.testing.trial.ProxyClient
import spock.lang.Specification

class MetamaskCallSpec extends Specification {

    def client = ProxyClient.forPrefix("eth")

    def "use long id"() {
        when:
        def act = client.execute(1057264140543346, "net_version", [])
        then:
        act.id == 1057264140543346
        act.result != null
        act.error == null
    }
}
