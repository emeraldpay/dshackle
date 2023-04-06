/**
 * Copyright (c) 2023 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.commons

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * A Reactor publishing Queue which keeps the data even if there is no subscriber, until the specified queueLimit is reached.
 * It is thread-safe, i.e. it can multiple (fire-and-forget) sources and multiple subscribers as well. For the case of
 * multiple subscriptions it doesn't share the original queue items, i.e., each subscriber get own set of original items.
 */
class QueuePublisher<T>(
    /**
     * Queue Limit. When the subscribers are slower that providers it starts to drop new items when reached this size
     */
    private val queueLimit: Int,
    private val sleepEmpty: Duration = Duration.ofMillis(5),
    onError: ((Error) -> Unit) = {}
) : Publisher<T> {

    var scheduler = Schedulers.boundedElastic()

    private val queue = ConcurrentLinkedQueue<T>()
    private val closed = AtomicBoolean(false)
    private val queueSize = AtomicInteger(0)
    private val nonFailingOnError: ((Error) -> Unit) = {
        try { onError(it) } catch (t: Throwable) {}
    }

    enum class Error {
        FULL, CLOSED, INTERNAL
    }

    /**
     * Current queue length
     */
    val size: Int
        get() = queueSize.get()

    /**
     * Put a new item into the queue
     */
    fun offer(value: T): Boolean {
        if (queueSize.get() >= queueLimit) {
            nonFailingOnError(Error.FULL)
            return false
        }
        if (closed.get()) {
            nonFailingOnError(Error.CLOSED)
            return false
        }
        return queue.offer(value).also {
            if (it) {
                queueSize.incrementAndGet()
            }
        }
    }

    /**
     * Close the queue, which also completes all current subscribers and cleans the queue.
     * Queue doesn't accept new items when closed.
     */
    fun close() {
        closed.set(true)
        queue.clear()
        queueSize.set(0)
    }

    override fun subscribe(client: Subscriber<in T>) {

        val cancelled = AtomicBoolean(false)
        val limit = AtomicLong(0)

        client.onSubscribe(object : Subscription {
            override fun request(n: Long) {
                limit.set(n)
                scheduler.schedule(Producer(cancelled, limit, client, this@QueuePublisher))
            }

            override fun cancel() {
                cancelled.set(true)
            }
        })
    }

    private class Producer<T>(
        val cancelled: AtomicBoolean,
        val limit: AtomicLong,
        val client: Subscriber<in T>,

        val parent: QueuePublisher<T>,
    ) : Runnable {
        override fun run() {
            while (!cancelled.get() && limit.get() > 0 && !parent.closed.get()) {
                val next = parent.queue.poll()
                if (next != null) {
                    parent.queueSize.decrementAndGet()
                    limit.decrementAndGet()
                    try {
                        client.onNext(next)
                    } catch (t: Throwable) {
                        // that's not supposed to happen. but what to do if it happened? closing the subscriber with an error is probably a good idea
                        client.onError(t)
                        parent.nonFailingOnError(Error.INTERNAL)
                        return
                    }
                } else {
                    parent.scheduler.schedule(this, parent.sleepEmpty.toMillis(), TimeUnit.MILLISECONDS)
                    return
                }
            }
            if (parent.closed.get()) {
                client.onComplete()
            } else {
                parent.scheduler.schedule(this, parent.sleepEmpty.toMillis(), TimeUnit.MILLISECONDS)
            }
        }
    }
}
