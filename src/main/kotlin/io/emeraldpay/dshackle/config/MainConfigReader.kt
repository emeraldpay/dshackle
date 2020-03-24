package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.FileResolver
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream

class MainConfigReader(
        fileResolver: FileResolver
) : YamlConfigReader(), ConfigReader<MainConfig> {

    companion object {
        private val log = LoggerFactory.getLogger(MainConfigReader::class.java)
    }

    private val authConfigReader = AuthConfigReader()
    private val proxyConfigReader = ProxyConfigReader()
    private val upstreamsConfigReader = UpstreamsConfigReader(fileResolver)
    private val cacheConfigReader = CacheConfigReader()

    fun read(input: InputStream): MainConfig? {
        val configNode = readNode(input)
        return read(configNode)
    }

    override fun read(input: MappingNode?): MainConfig? {
        val config = MainConfig()
        getValueAsString(input, "host")?.let {
            config.host = it
        }
        getValueAsInt(input, "port")?.let {
            config.port = it
        }

        authConfigReader.readServerTls(input)?.let {
            config.tls = it
        }
        proxyConfigReader.read(input)?.let {
            config.proxy = it
        }
        upstreamsConfigReader.read(input)?.let {
            config.upstreams = it
        }
        cacheConfigReader.read(input)?.let {
            config.cache = it
        }
        return config
    }

}