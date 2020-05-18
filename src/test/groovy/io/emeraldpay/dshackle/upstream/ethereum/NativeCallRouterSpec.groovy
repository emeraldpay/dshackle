package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.grpc.Chain
import spock.lang.Specification

import java.time.Duration

class NativeCallRouterSpec extends Specification {

    def "Calls hardcoded"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM)
        def router = new NativeCallRouter(
                new EthereumReader(
                        TestingCommons.aggregatedUpstream(TestingCommons.api()),
                        Caches.default()
                ),
                methods
        )
        when:
        def act = router.read(new JsonRpcRequest("eth_coinbase", [])).block(Duration.ofSeconds(1))
        then:
        act.resultAsProcessedString == "0x0000000000000000000000000000000000000000"
    }
}
