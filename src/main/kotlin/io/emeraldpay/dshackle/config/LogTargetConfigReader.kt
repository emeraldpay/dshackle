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
package io.emeraldpay.dshackle.config

import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode
import java.util.Locale

class LogTargetConfigReader(
    private val defaultFile: LogTargetConfig.File,
) : YamlConfigReader(),
    ConfigReader<LogTargetConfig.Any> {
    companion object {
        private val log = LoggerFactory.getLogger(LogTargetConfigReader::class.java)
    }

    override fun read(input: MappingNode?): LogTargetConfig.Any? {
        if (input == null) {
            return null
        }
        return when (val type = getValueAsString(input, "type")) {
            "file", null -> {
                var target = defaultFile.copy()
                getValueAsString(input, "filename", "file")?.let {
                    target = target.copy(filename = it)
                }
                return target
            }
            "socket" -> {
                var target =
                    LogTargetConfig.Socket(
                        host = "127.0.0.1",
                        port =
                            getValueAsInt(input, "port").also {
                                if (it == null) {
                                    log.error("Port must be specified for a Socket Log target")
                                }
                            }!!,
                    )
                getValueAsString(input, "host")?.let {
                    target = target.copy(host = it)
                }
                getValueAsString(input, "encoding")?.let { encodingId ->
                    val encoding =
                        when (encodingId.lowercase(Locale.getDefault())) {
                            "nl", "newline", "new-line", "new_line" -> LogTargetConfig.Encoding.NEW_LINE
                            "prefix", "length", "length_prefix", "length-prefix" -> LogTargetConfig.Encoding.SIZE_PREFIX
                            else -> null
                        }
                    if (encoding != null) {
                        target = target.copy(encoding = encoding)
                    }
                }
                getValueAsInt(input, "max_buffer", "max-buffer", "buffer")?.let { buffer ->
                    target = target.copy(bufferLimit = buffer)
                }
                return target
            }
            else -> {
                log.warn("Unknown log target type: $type")
                null
            }
        }
    }
}
