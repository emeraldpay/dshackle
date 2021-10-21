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

import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.CollectionNode
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream

class HealthConfigReader : YamlConfigReader(), ConfigReader<HealthConfig> {

    companion object {
        private val log = LoggerFactory.getLogger(HealthConfigReader::class.java)
    }

    fun read(input: InputStream): HealthConfig {
        val configNode = readNode(input)
        return read(configNode)
    }

    override fun read(input: MappingNode?): HealthConfig {
        return readInternal(getMapping(input, "health"))
    }

    fun readInternal(input: MappingNode?): HealthConfig {
        if (input == null) {
            return HealthConfig.default()
        }
        val config = HealthConfig()
        getValueAsString(input, "host")?.let {
            config.host = it
        }
        getValueAsInt(input, "port")?.let {
            config.port = it
        }
        getValueAsString(input, "path")?.let {
            config.path = it
        }
        readBlockchains(config, getList<MappingNode>(input, "blockchains"))
        return config
    }

    fun readBlockchains(healthConfig: HealthConfig, input: CollectionNode<MappingNode>?) {
        if (input == null) {
            return
        }
        input.value.forEach { conf ->
            val chain = getValueAsString(conf, "chain")
                ?.let { getBlockchain(it) }
            if (chain == null) {
                log.warn("Blockchain is not specified for a Health Check")
                return@forEach
            }
            if (chain == Chain.UNSPECIFIED) {
                log.warn("Using UNSPECIFIED blockchain for Health Check. Always fails")
            }
            if (healthConfig.chains.containsKey(chain)) {
                log.warn("Duplicate Health Check config for $chain. Replace previous with new")
            }
            val minAvailable = getValueAsInt(conf, "min-available") ?: 1
            healthConfig.chains[chain] = HealthConfig.ChainConfig(chain, minAvailable.coerceAtLeast(0))
        }
    }

}