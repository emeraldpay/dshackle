package io.emeraldpay.dshackle.testing.trial.basicproxy

import io.emeraldpay.dshackle.testing.trial.ProxyClient
import spock.lang.IgnoreIf
import spock.lang.Specification

@IgnoreIf({ System.getProperty('trialMode') != 'basic' })
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
