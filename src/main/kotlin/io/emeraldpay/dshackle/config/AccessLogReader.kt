package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.foundation.YamlConfigReader
import org.yaml.snakeyaml.nodes.MappingNode

class AccessLogReader : YamlConfigReader<AccessLogConfig>() {
    override fun read(input: MappingNode?): AccessLogConfig {
        return getMapping(input, "accessLog")?.let { node ->
            val enabled = getValueAsBool(node, "enabled") ?: false
            if (!enabled) {
                AccessLogConfig.disabled()
            } else {
                val includeMessages = getValueAsBool(node, "include-messages") ?: false
                val config = AccessLogConfig(true, includeMessages)
                getValueAsString(node, "filename")?.let {
                    config.filename = it
                }
                config
            }
        } ?: AccessLogConfig.default()
    }
}
