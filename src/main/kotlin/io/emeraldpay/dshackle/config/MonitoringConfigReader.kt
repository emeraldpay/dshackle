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
package io.emeraldpay.dshackle.config

import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream

class MonitoringConfigReader: YamlConfigReader(), ConfigReader<MonitoringConfig> {

    companion object {
        private val log = LoggerFactory.getLogger(MonitoringConfigReader::class.java)
    }

    fun read(input: InputStream): MonitoringConfig {
        val configNode = readNode(input)
        return read(configNode)
    }

    override fun read(input: MappingNode?): MonitoringConfig {
        return readInternal(getMapping(input, "monitoring"))
    }

    fun readInternal(input: MappingNode?): MonitoringConfig {
        if (input == null) {
            return MonitoringConfig.default()
        }
        val enabled = getValueAsBool(input, "enabled") ?: true
        if (!enabled) {
            return MonitoringConfig.disabled()
        }
        val prometheus = readPrometheus(getMapping(input, "prometheus"))
        return MonitoringConfig(enabled, prometheus).also { conf ->
            getValueAsBool(input, "enableJVM")?.let { conf.enableJvm = it }
        }
    }

    private fun readPrometheus(input: MappingNode?): MonitoringConfig.PrometheusConfig {
        if (input == null) {
            return MonitoringConfig.PrometheusConfig.default()
        }
        val enabled = getValueAsBool(input, "enabled") ?: true
        if (!enabled) {
            return MonitoringConfig.PrometheusConfig.disabled()
        }
        val default = MonitoringConfig.PrometheusConfig.default()
        val path = getValueAsString(input, "path") ?: default.path
        val host = getValueAsString(input, "bind") ?: getValueAsString(input, "host") ?: default.host
        val port = getValueAsInt(input, "port") ?: default.port
        return MonitoringConfig.PrometheusConfig(enabled, path, host, port)
    }

}