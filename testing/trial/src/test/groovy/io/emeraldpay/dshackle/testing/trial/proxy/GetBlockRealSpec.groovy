package io.emeraldpay.dshackle.testing.trial.proxy

import io.emeraldpay.dshackle.testing.trial.Debugger
import io.emeraldpay.dshackle.testing.trial.ProxyClient
import spock.lang.Specification
import spock.lang.Timeout

class GetBlockRealSpec extends Specification {

    def client = ProxyClient.ethereumReal()

    def setup() {
        Debugger.enabled = false
    }

    def cleanup() {
        Debugger.enabled = true
    }

    @Timeout(15)
    def "Correct order for transactions on #height"() {
        expect:
        def results = [
                (client.execute("eth_getBlockByNumber", ["0x" + Integer.toString(height, 16), false]).result["transactions"] as List<String>),
                (client.execute("eth_getBlockByNumber", ["0x" + Integer.toString(height, 16), true]).result["transactions"] as List<Map<String, Object>>)
                        .collect { it.hash }
        ]

        println(results[0])
        println(results[1])

        results[0] == results[1]

        where:
        height << (10_000_000..10_000_500)

    }
}
