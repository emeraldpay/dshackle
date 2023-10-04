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
package io.emeraldpay.dshackle.monitoring

import io.emeraldpay.dshackle.commons.DataQueue
import io.emeraldpay.dshackle.commons.RateLimitedAction
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.time.Duration

abstract class BufferingLogWriter<T>(
    private val serializer: LogSerializer<T>?,
    val encoding: LogEncoding,
    val queueLimit: Int = 4096,
    private val metrics: LogMetrics = LogMetrics.None(),
) : LogWriter<T> {

    companion object {
        private val log = LoggerFactory.getLogger(BufferingLogWriter::class.java)
    }

    protected var onFull: () -> Unit = {}

    private val errors = RateLimitedAction(Duration.ofSeconds(1))
    private val onQueueError: ((DataQueue.Error) -> Unit) = { err ->
        when (err) {
            DataQueue.Error.FULL -> {
                metrics.dropped()
                errors.execute { log.warn("Queue is full: ${queue.size}") }
                onFull()
            }
            DataQueue.Error.CLOSED -> errors.execute { log.warn("Queue is closed") }
            DataQueue.Error.INTERNAL -> errors.execute { log.warn("Queue cannot processes a log message") }
        }
    }
    private val queue = DataQueue<T>(queueLimit, onError = onQueueError)

    init {
        metrics.queue = queue
    }

    override fun submit(event: T) {
        queue.offer(event)
        metrics.produced()
    }

    override fun submitAll(events: List<T>) {
        queue.offer(events)
        events.forEach { _ -> metrics.produced() }
    }

    fun next(limit: Int): List<T> {
        return queue.request(limit)
    }

    fun returnBack(index: Int, events: List<T>) {
        queue.offer(events.drop(index))
        // don't fire the metrics update because it's they were already counted
    }

    fun encode(event: T): ByteBuffer? {
        val line = try {
            serializer?.apply(event)
        } catch (t: Throwable) {
            errors.execute {
                log.warn("Failed to serialize event: ${t.message}")
            }
            return null
        } ?: return null

        val encoded = try {
            encoding.write(line)
        } catch (t: Throwable) {
            errors.execute {
                log.warn("Failed to serialize event: ${t.message}")
            }
            null
        }
        return encoded
    }

    fun isEmpty(): Boolean {
        return queue.size <= 0
    }

    fun size(): Int {
        return queue.size
    }

    override fun stop() {
        queue.close()
    }
}
