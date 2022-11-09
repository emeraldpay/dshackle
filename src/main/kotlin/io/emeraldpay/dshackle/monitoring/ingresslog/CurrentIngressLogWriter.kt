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
package io.emeraldpay.dshackle.monitoring.ingresslog

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.monitoring.Channel
import io.emeraldpay.dshackle.monitoring.FileLogWriter
import io.emeraldpay.dshackle.monitoring.LogWriter
import io.emeraldpay.dshackle.monitoring.NoLogWriter
import io.emeraldpay.dshackle.monitoring.record.IngressRecord
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.LoggingJsonRpcReader
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.io.File
import java.time.Duration
import java.util.function.Function
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Repository
open class CurrentIngressLogWriter(
    @Autowired mainConfig: MainConfig,
) : IngressLogWriter {

    companion object {
        private val log = LoggerFactory.getLogger(CurrentIngressLogWriter::class.java)

        private const val WRITE_BATCH_LIMIT = 5000
        private val FLUSH_SLEEP = Duration.ofMillis(250L)
        private val START_SLEEP = Duration.ofMillis(1000L)
    }

    private val config = mainConfig.ingressLogConfig
    private val serializer = Function<IngressRecord.BlockchainRequest, ByteArray?> { next ->
        Global.objectMapper.writeValueAsBytes(next)
    }
    private val processor = IngressLogProcessor(this)

    var logWriter: LogWriter<IngressRecord.BlockchainRequest> = NoLogWriter()

    @PostConstruct
    fun start() {
        if (!config.enabled) {
            log.info("Ingress Log is disabled")
            return
        }

        // pass the current config, so it knows what to include into the log
        Global.monitoring.ingress.config = config

        val file = File(config.filename)
        log.info("Writing Ingress Log to ${file.absolutePath}")
        logWriter = FileLogWriter(
            file, serializer,
            startSleep = START_SLEEP, flushSleep = FLUSH_SLEEP,
            batchLimit = WRITE_BATCH_LIMIT
        )
        logWriter.start()
    }

    @PreDestroy
    fun flush() {
        logWriter.stop()
    }

    override fun accept(event: IngressRecord.BlockchainRequest) {
        logWriter.submit(event)
    }

    fun wrap(reader: JsonRpcReader, upstreamId: String, channel: Channel): JsonRpcReader {
        if (!config.enabled) {
            return reader
        }
        return LoggingJsonRpcReader(reader, processor.onComplete(upstreamId, channel))
    }
}
