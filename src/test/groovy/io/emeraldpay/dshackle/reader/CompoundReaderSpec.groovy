package io.emeraldpay.dshackle.reader

import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class CompoundReaderSpec extends Specification {

    def reader1 = new Reader<String, String>() {
        @Override
        Mono<String> read(String key) {
            return Mono.just("test-1").delaySubscription(Duration.ofMillis(100))
        }
    }
    def reader2 = new Reader<String, String>() {
        @Override
        Mono<String> read(String key) {
            return Mono.just("test-2").delaySubscription(Duration.ofMillis(200))
        }
    }
    def reader3 = new Reader<String, String>() {
        @Override
        Mono<String> read(String key) {
            return Mono.just("test-3").delaySubscription(Duration.ofMillis(300))
        }
    }

    def reader1Empty = new Reader<String, String>() {
        @Override
        Mono<String> read(String key) {
            return Mono.<String>empty().delaySubscription(Duration.ofMillis(100))
        }
    }

    def "Return empty when no readers"() {
        setup:
        def reader = new CompoundReader<String, String>()
        when:
        def act = reader.read("test")
        then:
        StepVerifier.create(act)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    def "Return first"() {
        setup:
        def reader = new CompoundReader<String, String>(reader1, reader2, reader3)
        when:
        def act = reader.read("test")
        then:
        StepVerifier.create(act)
                .expectNext("test-1")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Return second"() {
        setup:
        def reader = new CompoundReader<String, String>(reader3, reader2)
        when:
        def act = reader.read("test")
        then:
        StepVerifier.create(act)
                .expectNext("test-2")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Return third"() {
        setup:
        def reader = new CompoundReader<String, String>(reader3, reader2, reader1)
        when:
        def act = reader.read("test")
        then:
        StepVerifier.create(act)
                .expectNext("test-1")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Ignore empty"() {
        setup:
        def reader = new CompoundReader<String, String>(reader3, reader1Empty, reader2, reader1Empty)
        when:
        def act = reader.read("test")
        then:
        StepVerifier.create(act)
                .expectNext("test-2")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
