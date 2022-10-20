package io.emeraldpay.dshackle.upstream.bitcoin.subscribe


import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.SubscriptionConnect
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinUpstream
import reactor.core.publisher.Flux
import spock.lang.Specification

class BitcoinEgressSubscriptionSpec extends Specification {

    def "subscribe merges actual upstreams"() {
        setup:
        def multistream = Mock(BitcoinMultistream) {
            1 * upstreams >> [
                    Stub(BitcoinUpstream) {
                        getIngressSubscription() >> Stub(IngressSubscription) {
                            get("hashtx") >> Stub(SubscriptionConnect) {
                                connect() >> Flux.just("8b1b27f5c12f3ac7f8c4eaf5d0540e34ca3012782fdf002d8759400a308cbe6f")
                            }
                        }
                    },
                    Stub(BitcoinUpstream) {
                        getIngressSubscription() >> Stub(IngressSubscription) {
                            get("hashtx") >> Stub(SubscriptionConnect) {
                                connect() >> Flux.just(
                                        "8b1b27f5c12f3ac7f8c4eaf5d0540e34ca3012782fdf002d8759400a308cbe6f",
                                        "a3289cebdabc9352bb63fcffd3cfa04e45285952f9bce7bbb48ab6411657793e"
                                )
                            }
                        }
                    }
            ]
        }
        def subscription = new BitcoinEgressSubscription(multistream)
        when:
        def act = subscription.subscribe("hashtx", null)
            .collectList().block()

        then:
        act ==~ [
                "8b1b27f5c12f3ac7f8c4eaf5d0540e34ca3012782fdf002d8759400a308cbe6f",
                "8b1b27f5c12f3ac7f8c4eaf5d0540e34ca3012782fdf002d8759400a308cbe6f",
                "a3289cebdabc9352bb63fcffd3cfa04e45285952f9bce7bbb48ab6411657793e",
        ]
    }
}
