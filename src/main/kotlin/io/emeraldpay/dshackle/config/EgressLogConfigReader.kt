package io.emeraldpay.dshackle.config

import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode

class EgressLogConfigReader : YamlConfigReader(), ConfigReader<EgressLogConfig> {

    companion object {
        private val log = LoggerFactory.getLogger(EgressLogConfigReader::class.java)
    }

    override fun read(input: MappingNode?): EgressLogConfig {
        return getMapping(input, "egress-log", "egressLog", "access-log", "accessLog")?.let { node ->
            val enabled = getValueAsBool(node, "enabled") ?: false
            if (!enabled) {
                EgressLogConfig.disabled()
            } else {
                val includeMessages = getValueAsBool(node, "include-messages") ?: false
                val config = EgressLogConfig(true, includeMessages)
                getValueAsString(node, "filename")?.let {
                    config.filename = it
                }
                config
            }
        } ?: EgressLogConfig.default()
    }
}
