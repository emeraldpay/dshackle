package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class SubscribeStatusSpec extends Specification {

    def "gives UNAVAIL if non-configured chain is requested"() {
        setup:
        def ethereumUp = Mock(Upstream) {
            _ * getStatus() >> UpstreamAvailability.OK
        }
        def ethereumUpAll = Mock(Multistream) {
            _ * it.observeStatus() >> Mono.just(UpstreamAvailability.OK).repeat()
                    .delayElements(Duration.ofMillis(100))
            _ * it.getAll() >> [ethereumUp]
        }
        def ups = Mock(MultistreamHolder) {
            _ * it.getAvailable() >> [Chain.ETHEREUM]
            1 * it.getUpstream(Chain.ETHEREUM) >> ethereumUpAll
            1 * it.getUpstream(Chain.BITCOIN) >> null
        }
        def ctrl = new SubscribeStatus(ups)

        when:
        def req = BlockchainOuterClass.StatusRequest.newBuilder()
                .addChains(Common.ChainRef.CHAIN_ETHEREUM)
                .addChains(Common.ChainRef.CHAIN_BITCOIN)
                .build()
        def act = ctrl.subscribeStatus(Mono.just(req))
                .take(2)
        // sort just for testing
                .sort(new Comparator<BlockchainOuterClass.ChainStatus>() {
                    @Override
                    int compare(BlockchainOuterClass.ChainStatus o1, BlockchainOuterClass.ChainStatus o2) {
                        return o1.chainValue <=> o2.chainValue
                    }
                })
        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.chainValue == Chain.BITCOIN.id && it.availability == BlockchainOuterClass.AvailabilityEnum.AVAIL_UNAVAILABLE
                }
                .expectNextMatches {
                    it.chainValue == Chain.ETHEREUM.id && it.availability == BlockchainOuterClass.AvailabilityEnum.AVAIL_OK
                }
                .expectComplete()
                .verify(Duration.ofSeconds(3))
    }
}
