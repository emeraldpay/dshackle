package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import spock.lang.Specification

class ConnectNewHeadsSpec extends Specification {

    def "Reuse same head"() {
        setup:
        def head = Mock(Head) {
            1 * getFlux() >> Flux.fromIterable([
                    TestingCommons.blockForEthereum(100)
            ])
        }
        def up = Mock(EthereumMultistream) {
            1 * getHead() >> head
        }
        ConnectNewHeads connectNewHeads = new ConnectNewHeads(up)
        when:
        def act1 = connectNewHeads.connect()
        def act2 = connectNewHeads.connect()
        then:
        StepVerifier.create(act1)
                .expectNextCount(1)
                .expectComplete()
        StepVerifier.create(act2)
                .expectNextCount(1)
                .expectComplete()
    }
}
