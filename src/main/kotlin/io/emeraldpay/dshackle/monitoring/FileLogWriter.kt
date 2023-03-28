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

import org.slf4j.LoggerFactory
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class FileLogWriter<T>(
    private val file: File,
    private val serializer: (T) -> ByteArray?,
    private val startSleep: Duration,
    private val flushSleep: Duration,
    private val batchLimit: Int = 5000
) : LogWriter<T> {

    companion object {
        private val log = LoggerFactory.getLogger(FileLogWriter::class.java)

        private val NL = "\n".toByteArray()
    }

    private val scheduler = Executors.newSingleThreadScheduledExecutor()
    private val queue = ConcurrentLinkedQueue<T>()
    private var lastErrorAt: Instant = Instant.ofEpochMilli(0)
    private var started: Boolean = false

    private val runner = Runnable {
        flushRunner()
    }

    private fun flushRunner() {
        try {
            flush()
        } catch (t: Throwable) {
            logError {
                log.error("Failed to write logs. ${t.javaClass}:${t.message}")
            }
        } finally {
            scheduler.schedule(runner, flushSleep.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    override fun submit(event: T) {
        queue.add(event)
    }

    override fun submitAll(events: List<T>) {
        queue.addAll(events)
    }

    fun logError(m: () -> Unit) {
        val now = Instant.now()
        if (lastErrorAt.isBefore(now - Duration.ofMinutes(1))) {
            lastErrorAt = now
            m()
        }
    }

    fun flush(): Boolean {
        if (!file.exists()) {
            if (!file.createNewFile()) {
                logError {
                    log.error("Cannot create log file at ${file.absolutePath}")
                }
                return false
            }
        }
        BufferedOutputStream(FileOutputStream(file, true)).use { wrt ->
            var limit = batchLimit
            while (limit > 0) {
                limit -= 1
                val next = queue.poll() ?: return true
                val bytes = try {
                    serializer(next)
                } catch (t: Throwable) {
                    logError {
                        log.warn("Failed to serialize log line. ${t.message}")
                    }
                    null
                }

                if (bytes != null && bytes.isNotEmpty()) {
                    wrt.write(bytes)
                    wrt.write(NL, 0, 1)
                }
            }
        }
        return true
    }

    override fun start() {
        started = true
        scheduler.schedule(runner, startSleep.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun stop() {
        var tries = 10
        var failed = false
        while (tries > 0 && !failed && queue.isNotEmpty()) {
            tries--
            failed = flush()
        }
        started = false
    }

    override fun isRunning(): Boolean {
        return started
    }
}
