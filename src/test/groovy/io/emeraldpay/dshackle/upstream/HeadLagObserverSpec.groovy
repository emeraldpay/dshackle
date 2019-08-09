package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.test.StepVerifier
import reactor.util.function.Tuples
import spock.lang.Specification

import java.time.Duration

class HeadLagObserverSpec extends Specification {

    def "Updates lag distance"() {
        setup:
        EthereumHead master = Mock()

        EthereumHead head1 = Mock()
        EthereumHead head2 = Mock()

        Upstream up1 = Mock {
            _ * getHead() >> head1
        }
        Upstream up2 = Mock {
            _ * getHead() >> head2
        }

        def blocks = [100, 101, 102].collect { i ->
            return new BlockJson().with {
                it.number = i
                it.totalDifficulty = 2000 + i
                return it
            }
        }

        def masterBus = TopicProcessor.create()

        1 * master.getFlux() >> Flux.from(masterBus)
        1 * head1.getHead() >> Mono.just(blocks[1])
        1 * head1.getFlux() >> Flux.just(blocks[2])
                .delaySubscription(Duration.ofSeconds(1))
        1 * head2.getHead() >> Mono.just(blocks[0])
        1 * head2.getFlux() >> Flux.just(blocks[1])
                .delaySubscription(Duration.ofMillis(100))
        1 * up1.setLag(0)
        1 * up2.setLag(1)
        1 * up2.setLag(0)

        HeadLagObserver observer = new HeadLagObserver(master, [up1, up2])
        when:
        def act = observer.subscription().take(Duration.ofMillis(1200))

        then:
        StepVerifier.create(act)
            .then { masterBus.onNext(blocks[1]) }
            .expectNextCount(3)
            .verifyComplete()
    }

    def "Probes until there is no difference"() {
        setup:
        EthereumHead master = Mock()
        HeadLagObserver observer = new HeadLagObserver(master, [])
        Upstream up = Mock()

        def blocks = [100, 101, 102].collect { i ->
            return new BlockJson().with {
                it.number = i
                it.totalDifficulty = 2000 + i
                return it
            }
        }

        def upblocks = Flux.fromIterable(blocks)
        when:
        def act = observer.mapLagging(blocks[2], up, upblocks)
        then:
        StepVerifier.create(act)
                .expectNext(Tuples.of(2L, up))
                .expectNext(Tuples.of(1L, up))
                .expectNext(Tuples.of(0L, up))
                .verifyComplete()
    }

    def "Correct distance"() {
        setup:
        EthereumHead master = Mock()
        HeadLagObserver observer = new HeadLagObserver(master, [])
        expect:
        def top = new BlockJson().with {
            it.number = topHeight
            it.totalDifficulty = topDiff
            return it
        }
        def curr = new BlockJson().with {
            it.number = currHeight
            it.totalDifficulty = currDiff
            return it
        }
        delta as Long == observer.extractDistance(top, curr)
        where:
        topHeight | topDiff | currHeight | currDiff | delta
        100       | 1000    | 100        | 1000     | 0
        101       | 1010    | 100        | 1000     | 1
        102       | 1020    | 100        | 1000     | 2
        103       | 1030    | 100        | 1000     | 3
        150       | 1500    | 100        | 1000     | 50

        100       | 1000    | 101        | 1010     | 0
        100       | 1000    | 102        | 1020     | 0
        100       | 1000    | 100        | 1010     | 6
        100       | 1100    | 100        | 1000     | 6

    }
}
