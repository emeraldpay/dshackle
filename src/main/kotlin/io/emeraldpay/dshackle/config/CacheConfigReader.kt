/**
 * Copyright (c) 2020 EmeraldPay, Inc
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

class CacheConfigReader : YamlConfigReader(), ConfigReader<CacheConfig> {

    companion object {
        private val log = LoggerFactory.getLogger(CacheConfigReader::class.java)
    }

    fun read(input: InputStream): CacheConfig? {
        val configNode = readNode(input)
        return read(configNode)
    }

    override fun read(input: MappingNode?): CacheConfig? {
        return getMapping(input, "cache")?.let { node ->
            val config = CacheConfig()
            getMapping(node, "redis")?.let { node ->
                val redis = CacheConfig.Redis()
                getValueAsString(node, "host")?.let {
                    redis.host = it
                }
                getValueAsInt(node, "port")?.let {
                    redis.port = it
                }
                getValueAsInt(node, "db")?.let {
                    redis.db = it
                }
                getValueAsString(node, "password")?.let {
                    redis.password = it
                }
                config.redis = redis
            }
            if (config.redis == null) {
                return null
            }
            config
        }
    }
}