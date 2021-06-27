/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.MainConfig
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.io.*
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct

@Repository
class AccessLogWriter(
        @Autowired mainConfig: MainConfig
) {

    companion object {
        private val log = LoggerFactory.getLogger(AccessLogWriter::class.java)
        private const val WRITE_BATCH_LIMIT = 5000
        private const val FLUSH_SLEEP_MS = 500L
        private const val START_SLEEP_MS = 2000L
        private val NL = "\n".toByteArray()
    }

    private val config = mainConfig.accessLogConfig
    private val filename = File(config.filename)
    private val scheduler = Executors.newSingleThreadScheduledExecutor()

    private val queue = ConcurrentLinkedQueue<Any>()
    private val objectMapper = Global.objectMapper
    private var lastErrorAt: Instant = Instant.ofEpochMilli(0)

    private val runner = Runnable {
        flushRunner()
    }

    @PostConstruct
    fun start() {
        if (!config.enabled) {
            log.info("Access Log is disabled")
            return
        }
        log.info("Writing Access Log to ${filename.absolutePath}")
        scheduler.schedule(runner, START_SLEEP_MS, TimeUnit.MILLISECONDS)
    }

    private fun flushRunner() {
        try {
            flush()
        } catch (t: Throwable) {
            logError {
                log.error("Failed to write logs. ${t.javaClass}:${t.message}")
            }
        } finally {
            scheduler.schedule(runner, FLUSH_SLEEP_MS, TimeUnit.MILLISECONDS)
        }
    }

    fun submit(event: Any) {
        queue.add(event)
    }

    fun submit(events: List<Any>) {
        queue.addAll(events)
    }

    fun logError(m: () -> Unit) {
        val now = Instant.now()
        if (lastErrorAt.isBefore(now - Duration.ofMinutes(1))) {
            lastErrorAt = now
            m()
        }
    }

    protected fun flush() {
        if (!filename.exists()) {
            if (!filename.createNewFile()) {
                logError {
                    log.error("Cannot create Access Log file at ${filename.absolutePath}")
                }
                return
            }
        }
        BufferedOutputStream(FileOutputStream(filename, true)).use { wrt ->
            var limit = WRITE_BATCH_LIMIT
            while (limit > 0) {
                limit--
                val next = queue.poll() ?: return
                val bytes: ByteArray? = try {
                    objectMapper.writeValueAsBytes(next)
                } catch (t: Throwable) {
                    logError {
                        log.warn("Failed to write an access log line. ${t.message}")
                    }
                    null
                }
                if (bytes != null && bytes.isNotEmpty()) {
                    wrt.write(bytes)
                    wrt.write(NL, 0, 1)
                }
            }
        }
    }

}