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
package io.emeraldpay.dshackle.monitoring.egresslog

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.monitoring.FileLogWriter
import io.emeraldpay.dshackle.monitoring.LogWriter
import io.emeraldpay.dshackle.monitoring.NoLogWriter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.io.File
import java.time.Duration
import java.util.function.Function
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Repository
class CurrentEgressLogWriter(
    @Autowired mainConfig: MainConfig
) {

    companion object {
        private val log = LoggerFactory.getLogger(CurrentEgressLogWriter::class.java)
        private const val WRITE_BATCH_LIMIT = 5000
        private val FLUSH_SLEEP = Duration.ofMillis(250L)
        private val START_SLEEP = Duration.ofMillis(1000L)
    }

    private val config = mainConfig.egressLogConfig

    private val serializer = Function<Any, ByteArray?> { next ->
        Global.objectMapper.writeValueAsBytes(next)
    }

    var logWriter: LogWriter<Any> = NoLogWriter<Any>()

    @PostConstruct
    fun start() {
        if (!config.enabled) {
            log.info("Egress Log is disabled")
            return
        }
        val file = File(config.filename)
        log.info("Writing Egress Log to ${file.absolutePath}")
        logWriter = FileLogWriter<Any>(
            file, serializer,
            startSleep = START_SLEEP, flushSleep = FLUSH_SLEEP,
            batchLimit = WRITE_BATCH_LIMIT
        )
        logWriter.start()

        // propagate current config to the Event Builder, so it knows which details to include
        RecordBuilder.egressLogConfig = config
    }

    @PreDestroy
    fun flush() {
        logWriter.stop()
    }

    fun submit(event: Any) {
        logWriter.submit(event)
    }

    fun submitAll(events: List<Any>) {
        logWriter.submitAll(events)
    }
}
