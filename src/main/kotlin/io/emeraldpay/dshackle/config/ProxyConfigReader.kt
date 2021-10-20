/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2020 ETCDEV GmbH
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
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream

/**
 * Read YAML config, part related to Proxy configuration
 */
class ProxyConfigReader : YamlConfigReader(), ConfigReader<ProxyConfig> {

    companion object {
        private val log = LoggerFactory.getLogger(ProxyConfigReader::class.java)
    }

    private var filename = "dshackle.yaml"
    private val authConfigReader = AuthConfigReader()

    fun read(input: InputStream): ProxyConfig? {
        val configNode = readNode(input)
        return read(configNode)
    }

    override fun read(input: MappingNode?): ProxyConfig? {
        return readInternal(getMapping(input, "proxy"))
    }

    fun readInternal(input: MappingNode?): ProxyConfig? {
        if (input == null) {
            return null
        }
        val config = ProxyConfig()
        getValueAsString(input, "host")?.let {
            config.host = it
        }
        getValueAsInt(input, "port")?.let {
            config.port = it
        }
        getValueAsBool(input, "enabled")?.let {
            config.enabled = it
        }
        val currentRoutes = HashSet<String>()
        getList<MappingNode>(input, "routes")?.let { routes ->
            config.routes = routes.value.map { route ->
                val id = getValueAsString(route, "id")
                if (id == null || StringUtils.isEmpty(id) || !StringUtils.isAlphanumeric(id)) {
                    throw InvalidConfigYamlException(filename, route.startMark, "Route id must be alphanumeric")
                }
                if (currentRoutes.contains(id)) {
                    throw InvalidConfigYamlException(filename, route.startMark, "Route id repeated: $id")
                }
                currentRoutes.add(id)
                val blockchain = getValueAsString(route, "blockchain")
                if (StringUtils.isEmpty(blockchain) || getBlockchain(blockchain!!) == Chain.UNSPECIFIED) {
                    throw InvalidConfigYamlException(filename, route.startMark, "Invalid blockchain or not specified")
                }
                ProxyConfig.Route(id, getBlockchain(blockchain))
            }
        }
        if (config.routes.isEmpty()) {
            log.warn("Proxy config has no routes")
            return null
        }
        config.tls = authConfigReader.readServerTls(input)
        return config
    }
}
