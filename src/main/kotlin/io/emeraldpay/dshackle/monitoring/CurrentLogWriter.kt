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

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.LogTargetConfig
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.time.Duration
import java.util.Locale

abstract class CurrentLogWriter<T>(
    private val category: Category,
    private val serializer: LogSerializer<T>?,
    private val fileOptions: FileOptions? = null
) {

    companion object {
        private val log = LoggerFactory.getLogger(CurrentLogWriter::class.java)
    }

    var logWriter: LogWriter<T> = NoLogWriter<T>()

    fun setup(targetConfig: LogTargetConfig.Any?) {
        when (targetConfig) {
            null -> {
                logWriter = NoLogWriter()
            }

            is LogTargetConfig.File -> {
                val metrics = createMetrics()
                val file = Path.of(targetConfig.filename)
                log.info("Writing $category to $file")
                requireNotNull(fileOptions)
                logWriter = FileLogWriter<T>(
                    file, serializer,
                    startSleep = fileOptions.startSleep, flushSleep = fileOptions.flushSleep,
                    batchLimit = fileOptions.batchLimit,
                    metrics = metrics
                )
                logWriter.start()
            }

            is LogTargetConfig.Socket -> {
                val metrics = createMetrics()
                log.info("Sending $category to ${targetConfig.host}:${targetConfig.port}")
                val encoding = when (targetConfig.encoding) {
                    LogTargetConfig.Encoding.NEW_LINE -> LogEncodingNewLine()
                    LogTargetConfig.Encoding.SIZE_PREFIX -> LogEncodingPrefix()
                }
                logWriter = SocketLogWriter<T>(
                    targetConfig.host, targetConfig.port,
                    category,
                    serializer, encoding,
                    bufferSize = targetConfig.bufferLimit,
                    metrics = metrics
                )
            }
        }
    }

    private fun createMetrics() = if (Global.metricsExtended) {
        LogMetrics.Enabled(category.name.lowercase(Locale.getDefault()))
    } else {
        LogMetrics.None()
    }

    data class FileOptions(
        val startSleep: Duration,
        val flushSleep: Duration,
        val batchLimit: Int,
    )

    enum class Category {
        ACCESS, REQUEST
    }
}
