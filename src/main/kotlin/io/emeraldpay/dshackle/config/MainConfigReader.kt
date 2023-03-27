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
import org.yaml.snakeyaml.nodes.MappingNode

class MainConfigReader(
    fileResolver: FileResolver
) : YamlConfigReader<MainConfig>() {

    private val authConfigReader = AuthConfigReader()
    private val proxyConfigReader = ProxyConfigReader()
    private val upstreamsConfigReader = UpstreamsConfigReader(fileResolver)
    private val cacheConfigReader = CacheConfigReader()
    private val tokensConfigReader = TokensConfigReader()
    private val monitoringConfigReader = MonitoringConfigReader()
    private val accessLogReader = AccessLogReader()
    private val healthConfigReader = HealthConfigReader()
    private val signatureConfigReader = SignatureConfigReader(fileResolver)
    private val compressionConfigReader = CompressionConfigReader()
    private val chainsConfigReader = ChainsConfigReader(upstreamsConfigReader)

    override fun read(input: MappingNode?): MainConfig {
        val config = MainConfig()
        getValueAsString(input, "host")?.let {
            config.host = it
        }
        getValueAsInt(input, "port")?.let {
            config.port = it
        }

        getValueAsBool(input, "passthrough")?.let {
            config.passthrough = it
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
        accessLogReader.read(input).let {
            config.accessLogConfig = it
        }
        healthConfigReader.read(input).let {
            config.health = it
        }
        signatureConfigReader.read(input).let {
            config.signature = it
        }
        compressionConfigReader.read(input).let {
            config.compression = it
        }
        chainsConfigReader.read(input).let {
            config.chains = it
        }
        return config
    }
}
