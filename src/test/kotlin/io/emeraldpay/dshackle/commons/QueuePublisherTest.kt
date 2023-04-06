package io.emeraldpay.dshackle.commons

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class QueuePublisherTest : ShouldSpec({

    should("Produce current items") {
        val queue = QueuePublisher<Int>(100)

        StepVerifier.create(queue)
            .then {
                queue.offer(1)
                queue.offer(2)
            }
            .expectNext(1)
            .expectNext(2)
            .then {
                queue.offer(3)
            }
            .expectNext(3)
            .then {
                queue.offer(4)
            }
            .expectNext(4)
            .then {
                queue.close()
            }
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Produce queued items") {
        val queue = QueuePublisher<Int>(100)
        queue.offer(1)
        queue.offer(2)
        queue.offer(3)

        StepVerifier.create(queue)
            .expectNext(1)
            .expectNext(2)
            .expectNext(3)
            .then {
                queue.close()
            }
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Produce queued items and continue with fresh") {
        val queue = QueuePublisher<Int>(100)
        queue.offer(1)
        queue.offer(2)
        queue.offer(3)

        StepVerifier.create(queue)
            .expectNext(1)
            .expectNext(2)
            .expectNext(3)
            .then {
                queue.offer(4)
            }
            .expectNext(4)
            .then {
                queue.close()
            }
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Produce nothing when closed") {
        val queue = QueuePublisher<Int>(100)
        queue.offer(1)
        queue.offer(2)
        queue.offer(3)
        queue.close()

        StepVerifier.create(queue)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Have zero size when closed") {
        val queue = QueuePublisher<Int>(100)
        (1..5).forEach { i ->
            queue.offer(i)
        }
        queue.size shouldBeGreaterThan 0

        queue.close()
        (1..5).forEach { i ->
            queue.offer(i)
        }
        queue.size shouldBe 0
    }

    should("Produce items within a timeframe") {
        val queue = QueuePublisher<Int>(100)
        val t = Thread {
            (1..10).forEach { i ->
                queue.offer(i)
                Thread.sleep(100)
            }
        }

        val flux = Flux.from(queue)
            .take(Duration.ofMillis(350))

        StepVerifier.create(flux)
            .then { t.start() }
            .expectNext(1)
            .expectNext(2)
            .expectNext(3)
            .expectNext(4)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Produce X requested items") {
        val queue = QueuePublisher<Int>(100)
        val t = Thread {
            (1..10).forEach { i ->
                queue.offer(i)
                Thread.sleep(100)
            }
        }

        val flux = Flux.from(queue)
            .take(5)

        StepVerifier.create(flux)
            .then { t.start() }
            .expectNext(1)
            .expectNext(2)
            .expectNext(3)
            .expectNext(4)
            .expectNext(5)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Call onError when full") {
        val count = AtomicInteger(0)
        val queue = QueuePublisher<Int>(3, onError = { count.incrementAndGet() })
        val offers = mutableListOf<Boolean>()
        (1..5).forEach { i ->
            offers.add(queue.offer(i))
        }

        count.get() shouldBe 2
        queue.size shouldBe 3
        offers shouldBe listOf(true, true, true, false, false)
    }

    should("Call onError when closed") {
        val count = AtomicInteger(0)
        val queue = QueuePublisher<Int>(3, onError = { count.incrementAndGet() })
        val offers = mutableListOf<Boolean>()
        (1..2).forEach { i ->
            offers.add(queue.offer(i))
        }
        queue.close()
        (1..3).forEach { i ->
            offers.add(queue.offer(i))
        }

        count.get() shouldBe 3
        queue.size shouldBe 0
        offers shouldBe listOf(true, true, false, false, false)
    }

    should("Work with multiple threads") {
        val queue = QueuePublisher<Int>(100)
        val executor = Executors.newFixedThreadPool(6)
        executor.execute {
            (0 until 5).forEach { i ->
                queue.offer(i)
                Thread.sleep(19)
            }
        }
        executor.execute {
            (5 until 10).forEach { i ->
                queue.offer(i)
                Thread.sleep(23)
            }
        }
        executor.execute {
            (10 until 15).forEach { i ->
                queue.offer(i)
                Thread.sleep(27)
            }
        }
        executor.execute {
            (15 until 20).forEach { i ->
                queue.offer(i)
                Thread.sleep(31)
            }
        }

        val list1 = mutableListOf<Int>()
        val list2 = mutableListOf<Int>()
        executor.execute {
            Flux.from(queue).take(10).subscribe(list1::add)
        }
        executor.execute {
            Flux.from(queue).take(10).subscribe(list2::add)
        }

        executor.shutdown()
        executor.awaitTermination(1, TimeUnit.SECONDS)

        list1 shouldHaveAtLeastSize 1
        list2 shouldHaveAtLeastSize 1
        (list1 + list2) shouldContainAll (0 until 20).toList()
    }
})
