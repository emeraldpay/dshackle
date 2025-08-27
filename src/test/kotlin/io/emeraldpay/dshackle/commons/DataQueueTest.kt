package io.emeraldpay.dshackle.commons

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class DataQueueTest :
    ShouldSpec({

        should("Produce current items") {
            val queue = DataQueue<Int>(100)

            queue.offer(1)
            queue.offer(2)

            queue.request(1) shouldBe listOf(1)
            queue.request(1) shouldBe listOf(2)

            queue.offer(3)
            queue.request(1) shouldBe listOf(3)

            queue.offer(4)
            queue.request(1) shouldBe listOf(4)

            queue.close()
        }

        should("Produce queued items") {
            val queue = DataQueue<Int>(100)
            queue.offer(1)
            queue.offer(2)
            queue.offer(3)

            queue.request(10) shouldBe listOf(1, 2, 3)
        }

        should("Produce queued items and continue with fresh") {
            val queue = DataQueue<Int>(100)
            queue.offer(1)
            queue.offer(2)
            queue.offer(3)

            queue.request(10) shouldBe listOf(1, 2, 3)

            queue.offer(4)
            queue.request(10) shouldBe listOf(4)
        }

        should("Produce nothing when closed") {
            val queue = DataQueue<Int>(100)
            queue.offer(1)
            queue.offer(2)
            queue.offer(3)
            queue.close()

            queue.request(10) shouldBe emptyList()
        }

        should("Have zero size when closed") {
            val queue = DataQueue<Int>(100)
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

        should("Produce X requested items") {
            val queue = DataQueue<Int>(100)
            (1..10).forEach { i ->
                queue.offer(i)
            }

            queue.request(5) shouldBe listOf(1, 2, 3, 4, 5)
            queue.request(3) shouldBe listOf(6, 7, 8)
            queue.request(2) shouldBe listOf(9, 10)
        }

        should("Call onError when full") {
            val count = AtomicInteger(0)
            val queue = DataQueue<Int>(3, onError = { count.incrementAndGet() })
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
            val queue = DataQueue<Int>(3, onError = { count.incrementAndGet() })
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
            val queue = DataQueue<Int>(100)
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
                repeat(15) {
                    queue.request(1).forEach(list1::add)
                    Thread.sleep(7)
                }
            }
            executor.execute {
                repeat(15) { _ ->
                    queue.request(1).forEach(list2::add)
                    Thread.sleep(11)
                }
            }

            executor.shutdown()
            executor.awaitTermination(1, TimeUnit.SECONDS)

            list1 shouldHaveAtLeastSize 1
            list2 shouldHaveAtLeastSize 1
            (list1 + list2) shouldContainAll (0 until 20).toList()
        }
    })
