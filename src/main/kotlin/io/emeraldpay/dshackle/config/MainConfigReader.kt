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
    private val tokensConfigReader = TokensConfigReader()
    private val monitoringConfigReader = MonitoringConfigReader()
    private val accessLogConfigReader = AccessLogConfigReader()
    private val requestLogConfigReader = RequestLogConfigReader()
    private val healthConfigReader = HealthConfigReader()
    private val signatureConfigReader = SignatureConfigReader(fileResolver)

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
        getValueAsBool(input, "compress")?.let {
            config.compress = it
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
        tokensConfigReader.read(input)?.let {
            config.tokens = it
        }
        monitoringConfigReader.read(input).let {
            config.monitoring = it
        }
        accessLogConfigReader.read(input).let {
            config.accessLogConfig = it
        }
        requestLogConfigReader.read(input).let {
            config.requestLogConfig = it
        }
        healthConfigReader.read(input).let {
            config.health = it
        }
        signatureConfigReader.read(input).let {
            config.signature = it
        }
        return config
    }
}
