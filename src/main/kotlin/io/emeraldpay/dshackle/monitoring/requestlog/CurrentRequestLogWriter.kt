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
package io.emeraldpay.dshackle.monitoring.requestlog

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.monitoring.Channel
import io.emeraldpay.dshackle.monitoring.CurrentLogWriter
import io.emeraldpay.dshackle.monitoring.record.RequestRecord
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.LoggingJsonRpcReader
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.time.Duration
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Repository
open class CurrentRequestLogWriter(
    @Autowired mainConfig: MainConfig,
) : RequestLogWriter, CurrentLogWriter<RequestRecord.BlockchainRequest>(
    Category.REQUEST, serializer,
    FileOptions(startSleep = START_SLEEP, flushSleep = FLUSH_SLEEP, batchLimit = WRITE_BATCH_LIMIT)
) {

    companion object {
        private val log = LoggerFactory.getLogger(CurrentRequestLogWriter::class.java)

        private const val WRITE_BATCH_LIMIT = 5000
        private val FLUSH_SLEEP = Duration.ofMillis(250L)
        private val START_SLEEP = Duration.ofMillis(1000L)

        private val serializer: (RequestRecord.BlockchainRequest) -> ByteArray? = { next ->
            Global.objectMapper.writeValueAsBytes(next)
        }
    }

    private val config = mainConfig.requestLogConfig

    private val processor = IngressLogProcessor(this)

    @PostConstruct
    fun start() {
        if (!config.enabled) {
            log.info("Request Log is disabled")
            return
        }

        setup(config.target)
        logWriter.start()

        // pass the current config, so it knows what to include into the log
        Global.monitoring.ingress.config = config
    }

    @PreDestroy
    fun flush() {
        logWriter.stop()
    }

    override fun accept(event: RequestRecord.BlockchainRequest) {
        logWriter.submit(event)
    }

    fun wrap(reader: StandardRpcReader, upstreamId: String, channel: Channel): StandardRpcReader {
        if (!config.enabled) {
            return reader
        }
        return LoggingJsonRpcReader(reader, processor.onComplete(upstreamId, channel))
    }
}
