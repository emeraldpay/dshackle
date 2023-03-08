package io.emeraldpay.dshackle.reader

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import reactor.core.publisher.Mono
import java.time.Duration

class CompoundReaderTest : ShouldSpec({

    val reader1 = object : Reader<String, String> {
        override fun read(key: String): Mono<String> {
            return Mono.just("test-1").delaySubscription(Duration.ofMillis(100))
        }
    }
    val reader2 = object : Reader<String, String> {
        override fun read(key: String): Mono<String> {
            return Mono.just("test-2").delaySubscription(Duration.ofMillis(200))
        }
    }
    val reader3 = object : Reader<String, String> {
        override fun read(key: String): Mono<String> {
            return Mono.just("test-3").delaySubscription(Duration.ofMillis(300))
        }
    }

    val reader1Empty = object : Reader<String, String> {
        override fun read(key: String): Mono<String> {
            return Mono.empty<String>().delaySubscription(Duration.ofMillis(100))
        }
    }

    should("Return empty when no readers") {
        val reader = CompoundReader<String, String>()

        val act = reader.read("test")
            .block(Duration.ofSeconds(1))

        act shouldBe null
    }

    should("Return first") {
        val reader = CompoundReader(reader1, reader2, reader3)

        val act = reader.read("test")
            .block(Duration.ofSeconds(1))

        act shouldBe "test-1"
    }

    should("Not call others after getting first") {
        var call2 = false
        val readerTrack = object : Reader<String, String> {
            override fun read(key: String): Mono<String> {
                call2 = true
                return Mono.just("test-2").delaySubscription(Duration.ofMillis(200))
            }
        }

        val reader = CompoundReader(reader1, readerTrack)

        val act = reader.read("test")
            .block(Duration.ofSeconds(1))

        act shouldBe "test-1"
        call2 shouldBe false
    }

    should("Return first even if it's slow") {
        val reader = CompoundReader(reader3, reader2)

        val act = reader.read("test")
            .block(Duration.ofSeconds(1))

        act shouldBe "test-3"
    }

    should("Ignore empty") {
        val reader = CompoundReader(reader1Empty, reader3, reader2, reader1Empty)

        val act = reader.read("test")
            .block(Duration.ofSeconds(1))

        act shouldBe "test-3"
    }

    should("Stop on error") {
        val readerFailing = object : Reader<String, String> {
            override fun read(key: String): Mono<String> {
                return Mono.error(RuntimeException("Test Error"))
            }
        }
        val reader = CompoundReader(reader1Empty, readerFailing, reader1)

        val act = shouldThrow<RuntimeException> {
            reader.read("test")
                .block(Duration.ofSeconds(1))
        }

        act shouldNotBe null
        act.message!! shouldBe "Test Error"
    }
})
