/**
 * Copyright (c) 2022 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.commons.RateLimitedAction
import org.slf4j.LoggerFactory
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class FileLogWriter<T>(
    private val file: Path,
    serializer: LogSerializer<T>?,
    private val startSleep: Duration,
    private val flushSleep: Duration,
    private val batchLimit: Int = 5000,
    metrics: LogMetrics = LogMetrics.None(),
) : LogWriter<T>, BufferingLogWriter<T>(serializer, LogEncodingNewLine(), queueLimit = batchLimit, metrics = metrics) {

    companion object {
        private val log = LoggerFactory.getLogger(FileLogWriter::class.java)
    }
    private val scheduler = Executors.newSingleThreadScheduledExecutor()
    private var started: Boolean = false
    private val batchTime = flushSleep.dividedBy(2).coerceAtLeast(Duration.ofMillis(5))
    private val errors = RateLimitedAction(Duration.ofSeconds(1))
    private val flushLock = Semaphore(1)

    private val runner = Runnable {
        flushRunner()
    }

    init {
        check(flushSleep >= Duration.ofMillis(10)) {
            "Flush sleep is too small: ${flushSleep.toMillis()}ms <= 10ms"
        }
    }

    private fun flushRunner() {
        try {
            flush()
        } catch (t: Throwable) {
            errors.execute {
                log.error("Failed to write logs. ${t.javaClass}:${t.message}")
            }
        } finally {
            scheduler.schedule(runner, flushSleep.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    fun flush(): Boolean {
        if (!flushLock.tryAcquire(1, batchTime.toMillis(), TimeUnit.MILLISECONDS)) {
            return false
        }

        try {
            val channel = try {
                FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE)
            } catch (t: Throwable) {
                errors.execute {
                    log.error("Cannot create log file at $file")
                }
                return false
            }

            return channel.use { wrt ->
                try {
                    super.readEncodedFromQueue()
                        .map {
                            wrt.write(it)
                            true
                        }
                        .take(size().coerceAtMost(batchLimit).toLong())
                        .take(batchTime)
                        // complete writes in this thread _blocking_
                        .blockLast()
                    true
                } catch (t: Throwable) {
                    errors.execute { log.warn("Failed to write to the log", t) }
                    false
                }
            }
        } finally {
            flushLock.release()
        }
    }

    override fun start() {
        started = true
        scheduler.schedule(runner, startSleep.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun stop() {
        var tries = 10
        var failed = false
        while (tries > 0 && !failed && !isEmpty()) {
            tries--
            failed = !flush()
        }
        started = false
        super.stop()
    }

    override fun isRunning(): Boolean {
        return started
    }
}
