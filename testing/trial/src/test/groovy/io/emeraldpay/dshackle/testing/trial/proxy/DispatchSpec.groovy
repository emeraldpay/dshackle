package io.emeraldpay.dshackle.testing.trial.proxy

import io.emeraldpay.dshackle.testing.trial.ProxyClient
import spock.lang.Specification

class DispatchSpec extends Specification {

    def client = ProxyClient.forPrefix("eth")

    def "multiple calls routed roughly equal to upstreams"() {
        when:
        def calls1before = ProxyClient.forOriginal(18545).execute("eth_call", [[to: "0x0123456789abcdef0123456789abcdef00000002"]]).result as List
        def calls2before = ProxyClient.forOriginal(18546).execute("eth_call", [[to: "0x0123456789abcdef0123456789abcdef00000002"]]).result as List

        100.times {
            client.execute("eth_call", [[to: "0x0123456789abcdef0123456789abcdef00000001", data: "0x00000000" + Integer.toString(it, 16)]])
        }

        def calls1after = ProxyClient.forOriginal(18545).execute("eth_call", [[to: "0x0123456789abcdef0123456789abcdef00000002"]]).result as List
        def calls2after = ProxyClient.forOriginal(18546).execute("eth_call", [[to: "0x0123456789abcdef0123456789abcdef00000002"]]).result as List

        def calls1 = onlyNew(calls1before, calls1after)
        def calls2 = onlyNew(calls2before, calls2after)

        then:
        calls1.size() >= 48
        calls2.size() >= 48
        calls1.size() + calls2.size() == 100
    }

    private List onlyNew(List before, List after) {
        return after.findAll { a ->
            !before.any { b ->
                b.id == a.id
            }
        }.findAll {
            (it.json as String).contains("0x0123456789abcdef0123456789abcdef00000001")
        }
    }
}
