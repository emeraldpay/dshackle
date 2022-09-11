package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import reactor.test.scheduler.VirtualTimeScheduler
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class OptionalHeadSpec extends Specification {

    static def TEST_BLOCKS = [1L, 2, 3, 4].collect { i ->
        byte[] hash = new byte[32]
        hash[0] = i as byte
        new BlockContainer(i, BlockId.from(hash), BigInteger.valueOf(i), Instant.now())
    }

    def "Doesn't use delegate if not enabled"() {
        setup:
        def delegate = Mock(Head) {
            0 * getFlux()
            0 * getCurrentHeight()
        }
        def head = new OptionalHead(delegate)

        when:
        def height = head.getCurrentHeight()
        then:
        height == null

        when:
        def blocks = head.getFlux()
        then:
        StepVerifier.withVirtualTime { blocks.take(Duration.ofSeconds(5)) }
            .thenAwait(Duration.ofSeconds(5))
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    def "Switch to delegate when gets enabled"() {
        setup:
        def delegate = Mock(Head) {
            1 * getFlux() >> Flux.fromIterable(TEST_BLOCKS)
                    .delayElements(Duration.ofSeconds(15), VirtualTimeScheduler.getOrSet())
        }
        def head = new OptionalHead(delegate)

        when:
        def blocks = head.getFlux()
        then:
        StepVerifier.withVirtualTime { blocks.take(Duration.ofMinutes(1)) }
                .thenAwait(Duration.ofSeconds(5))
                .then { head.setEnabled(true) }
                .thenAwait(Duration.ofSeconds(15))
                .expectNext(TEST_BLOCKS[0]).as("Received 0")
                .thenAwait(Duration.ofSeconds(15))
                .expectNext(TEST_BLOCKS[1]).as("Received 1")
                .thenAwait(Duration.ofSeconds(15))
                .expectNext(TEST_BLOCKS[2]).as("Received 2")
                .thenAwait(Duration.ofSeconds(10))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }


    def "Switch to nothing when gets disabled"() {
        setup:
        def delegate = Mock(Head) {
            1 * getFlux() >> Flux.fromIterable(TEST_BLOCKS)
                    .delayElements(Duration.ofSeconds(15), VirtualTimeScheduler.getOrSet())
        }
        def head = new OptionalHead(delegate)

        when:
        def blocks = head.getFlux()
        then:
        StepVerifier.withVirtualTime { blocks.take(Duration.ofMinutes(1)) }
                .thenAwait(Duration.ofSeconds(5))
                .then { head.setEnabled(true) }
                .thenAwait(Duration.ofSeconds(15))
                .expectNext(TEST_BLOCKS[0]).as("Received 0")
                .thenAwait(Duration.ofSeconds(15))
                .expectNext(TEST_BLOCKS[1]).as("Received 1")
                .then { head.setEnabled(false) }
                .thenAwait(Duration.ofSeconds(25))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
