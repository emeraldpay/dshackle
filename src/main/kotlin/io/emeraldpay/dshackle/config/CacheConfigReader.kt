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

import io.emeraldpay.dshackle.foundation.YamlConfigReader
import org.yaml.snakeyaml.nodes.MappingNode

class CacheConfigReader : YamlConfigReader<CacheConfig>() {

    override fun read(input: MappingNode?): CacheConfig? {
        return getMapping(input, "cache")?.let { node ->
            val config = CacheConfig()
            getMapping(node, "redis")?.let { redisNode ->
                val redis = CacheConfig.Redis()
                val enabled = getValueAsBool(redisNode, "enabled") ?: true
                if (enabled) {
                    getValueAsString(redisNode, "host")?.let {
                        redis.host = it
                    }
                    getValueAsInt(redisNode, "port")?.let {
                        redis.port = it
                    }
                    getValueAsInt(redisNode, "db")?.let {
                        redis.db = it
                    }
                    getValueAsString(redisNode, "password")?.let {
                        redis.password = it
                    }
                    config.redis = redis
                }
            }
            if (config.redis == null) {
                return null
            }
            config
        }
    }
}
