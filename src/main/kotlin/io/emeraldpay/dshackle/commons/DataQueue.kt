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

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A Queue for data for further processing.
 * It is thread-safe, i.e. it can have multiple (fire-and-forget) providers and multiple readers. For the case of
 * multiple readers it doesn't share the original queue items, i.e., each reader get own set of original items.
 */
class DataQueue<T>(
    /**
     * Queue Limit. When the subscribers are slower that providers it starts to drop new items when reached this size
     */
    private val queueLimit: Int,
    onError: ((Error) -> Unit) = {}
) {

    private val locks = ReentrantLock()
    private var queue = ArrayList<T>()
    private var closed = false
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
        get() = locks.withLock { queue.size }

    /**
     * Put a new item into the queue
     */
    fun offer(values: List<T>): Boolean {
        locks.withLock {
            if (queue.size >= queueLimit) {
                nonFailingOnError(Error.FULL)
                return false
            }
            if (closed) {
                nonFailingOnError(Error.CLOSED)
                return false
            }
            queue.addAll(values)
            return true
        }
    }

    fun offer(value: T): Boolean {
        locks.withLock {
            if (queue.size >= queueLimit) {
                nonFailingOnError(Error.FULL)
                return false
            }
            if (closed) {
                nonFailingOnError(Error.CLOSED)
                return false
            }
            queue.add(value)
            return true
        }
    }

    /**
     * Close the queue, which also clears the queue.
     * Queue doesn't accept new items when closed.
     */
    fun close() {
        locks.withLock {
            closed = true
            queue.clear()
        }
    }

    fun request(limit: Int): List<T> {
        return locks.withLock {
            if (queue.isEmpty()) {
                emptyList()
            } else if (queue.size < limit) {
                val copy = queue
                queue = ArrayList(copy.size)
                copy
            } else {
                val copy = queue
                val back = ArrayList<T>(queue.size)
                val result = ArrayList<T>(limit)
                for (i in 0 until limit) {
                    result.add(copy[i])
                }
                for (i in limit until copy.size) {
                    back.add(copy[i])
                }
                queue = back
                result
            }
        }
    }
}
