package io.emeraldpay.dshackle.config

import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode

class AccessLogReader : YamlConfigReader(), ConfigReader<AccessLogConfig> {

    companion object {
        private val log = LoggerFactory.getLogger(AccessLogReader::class.java)
    }

    override fun read(input: MappingNode?): AccessLogConfig {
        return getMapping(input, "accessLog")?.let { node ->
            val enabled = getValueAsBool(node, "enabled") ?: false
            if (!enabled) {
                AccessLogConfig.disabled()
            } else {
                val config = AccessLogConfig(true)
                getValueAsString(node, "filename")?.let {
                    config.filename = it
                }
                config
            }
        } ?: AccessLogConfig.default()
    }

}