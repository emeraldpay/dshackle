package io.emeraldpay.dshackle.upstream.bitcoin.subscribe


import reactor.core.publisher.Flux
import spock.lang.Specification

class BitcoinRpcIngressSubscriptionSpec extends Specification {

    def "connect correct upstream"() {
        setup:
        def subscriptionList = [
                Stub(BitcoinSubscriptionConnect) {
                    topic >> BitcoinZmqTopic.HASHTX
                    connect() >> Flux.just("8b1b27f5c12f3ac7f8c4eaf5d0540e34ca3012782fdf002d8759400a308cbe6f")
                },
                Stub(BitcoinSubscriptionConnect) {
                    topic >> BitcoinZmqTopic.HASHBLOCK
                    connect() >> Flux.just(
                            "00000000000000000002504f321ef8005506869b58d41fde1c0fb908dabd8a46",
                            "0000000000000000000540c8ffb2ab013bb3bc7ae2852f9fc97e42a3c2370a6d"
                    )
                }
        ]
        def subscription = new BitcoinRpcIngressSubscription(subscriptionList)
        when:
        def act = subscription.get("hashblock")
                .connect()
                .collectList()
                .block()

        then:
        act == [
                "00000000000000000002504f321ef8005506869b58d41fde1c0fb908dabd8a46",
                "0000000000000000000540c8ffb2ab013bb3bc7ae2852f9fc97e42a3c2370a6d",
        ]
    }
}
