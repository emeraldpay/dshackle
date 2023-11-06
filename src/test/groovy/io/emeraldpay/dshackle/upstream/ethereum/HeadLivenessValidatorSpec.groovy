package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.test.EthereumHeadMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.Head
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class HeadLivenessValidatorSpec extends Specification{
    def "emits true"() {
        when:
        def head = new EthereumHeadMock()
        def checker = new HeadLivenessValidator(head, Duration.ofSeconds(10), Schedulers.boundedElastic(), "test")
        then:
        StepVerifier.create(checker.flux)
                .then {
                    head.nextBlock(TestingCommons.blockForEthereum(1))
                    head.nextBlock(TestingCommons.blockForEthereum(2))
                    head.nextBlock(TestingCommons.blockForEthereum(3))
                }.expectNext(true).thenCancel().verify(Duration.ofSeconds(1))
    }

    def "emits false if head liveness emits false"() {
        when:
        def head = Mock(Head) {
            1 * it.headLiveness() >> Flux.just(false)
            1 * it.getFlux() >> Flux.just(TestingCommons.blockForEthereum(1))
        }
        def checker = new HeadLivenessValidator(head, Duration.ofSeconds(10), Schedulers.boundedElastic(), "test")
        then:
        StepVerifier.create(checker.flux)
                .expectNext(false)
                .thenCancel()
                .verify(Duration.ofSeconds(1))
    }

    def "starts accumulating trues but immediately emits after false"() {
        when:
        def head = new EthereumHeadMock()
        def checker = new HeadLivenessValidator(head, Duration.ofSeconds(100), Schedulers.boundedElastic(), "test")
        then:
        StepVerifier.create(checker.flux)
                .then {
                    head.nextBlock(TestingCommons.blockForEthereum(1))
                    head.nextBlock(TestingCommons.blockForEthereum(2))
                }
                .expectNoEvent(Duration.ofMillis(100))
                .then {
                    head.nextBlock(TestingCommons.blockForEthereum(5))
                }
                .expectNext(false)
                .thenCancel().verify(Duration.ofSeconds(1))
    }

    def "starts accumulating trues but timeouts because head staled"() {
        when:
        def head = new EthereumHeadMock()
        def checker = new HeadLivenessValidator(head, Duration.ofMillis(100), Schedulers.boundedElastic(), "test")
        then:
        StepVerifier.create(checker.flux)
                .then {
                    head.nextBlock(TestingCommons.blockForEthereum(1))
                    head.nextBlock(TestingCommons.blockForEthereum(2))
                }
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(false)
                .thenCancel().verify(Duration.ofSeconds(2))
    }

    def "it recovers after timeout"() {
        when:
        def head = new EthereumHeadMock()
        def checker = new HeadLivenessValidator(head, Duration.ofMillis(200), Schedulers.boundedElastic(), "test")
        then:
        StepVerifier.create(checker.flux)
                .then {
                    head.nextBlock(TestingCommons.blockForEthereum(1))
                    head.nextBlock(TestingCommons.blockForEthereum(2))
                }
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(false)
                .then {
                    head.nextBlock(TestingCommons.blockForEthereum(3))
                    head.nextBlock(TestingCommons.blockForEthereum(4))
                    head.nextBlock(TestingCommons.blockForEthereum(5))
                }
                .expectNext(true)
                .thenCancel().verify(Duration.ofSeconds(3))
    }
}
