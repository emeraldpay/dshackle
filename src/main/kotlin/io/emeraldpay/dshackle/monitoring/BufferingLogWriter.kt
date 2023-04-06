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

import io.emeraldpay.dshackle.commons.QueuePublisher
import io.emeraldpay.dshackle.commons.RateLimitedAction
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.nio.ByteBuffer
import java.time.Duration
import java.util.Optional

abstract class BufferingLogWriter<T>(
    private val serializer: (T) -> ByteArray?,
    val encoding: LogEncoding,
    val queueLimit: Int = 4096,
    private val metrics: LogMetrics = LogMetrics.None(),
) : LogWriter<T> {

    companion object {
        private val log = LoggerFactory.getLogger(BufferingLogWriter::class.java)
    }

    protected var onFull: () -> Unit = {}

    private val errors = RateLimitedAction(Duration.ofSeconds(1))
    private val onQueueError: ((QueuePublisher.Error) -> Unit) = { err ->
        when (err) {
            QueuePublisher.Error.FULL -> {
                metrics.dropped()
                errors.execute { log.warn("Queue is full: ${queue.size}") }
                onFull()
            }
            QueuePublisher.Error.CLOSED -> errors.execute { log.warn("Queue is closed") }
            QueuePublisher.Error.INTERNAL -> errors.execute { log.warn("Queue cannot processes a log message") }
        }
    }
    private val queue = QueuePublisher<T>(queueLimit, onError = onQueueError)

    override fun submit(event: T) {
        metrics.produced()
        queue.offer(event)
    }

    override fun submitAll(events: List<T>) {
        events.forEach(::submit)
    }

    fun readFromQueue(): Flux<T> {
        return Flux.from(queue)
    }

    fun readEncodedFromQueue(): Flux<ByteBuffer> {
        return readFromQueue()
            .map(::toByteBuffer)
            .filter(Optional<ByteBuffer>::isPresent)
            .map(Optional<ByteBuffer>::get)
    }

    fun toByteBuffer(event: T): Optional<ByteBuffer> {
        val line = try {
            serializer(event)
        } catch (t: Throwable) {
            errors.execute {
                log.warn("Failed to serialize event: ${t.message}")
            }
            null
        } ?: return Optional.empty()

        val encoded = try {
            encoding.write(line)
        } catch (t: Throwable) {
            errors.execute {
                log.warn("Failed to serialize event: ${t.message}")
            }
            null
        }
        return Optional.ofNullable(encoded)
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
