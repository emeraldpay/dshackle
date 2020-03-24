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