/**
 * Copyright (c) 2022 EmeraldPay, Inc
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

class RequestLogConfigReader : YamlConfigReader(), ConfigReader<RequestLogConfig> {

    companion object {
        private val log = LoggerFactory.getLogger(RequestLogConfigReader::class.java)
    }

    override fun read(input: MappingNode?): RequestLogConfig {
        return getMapping(input, "request-log", "requestLog", "ingress-log", "ingressLog")?.let { node ->
            val enabled = getValueAsBool(node, "enabled") ?: false
            if (!enabled) {
                RequestLogConfig.disabled()
            } else {
                val includeParams = getValueAsBool(node, "include-params") ?: false
                val config = RequestLogConfig(true, includeParams)
                LogTargetConfigReader(RequestLogConfig.defaultFile).read(node)?.let {
                    config.target = it
                }
                config
            }
        } ?: RequestLogConfig.default()
    }
}
