package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.foundation.YamlConfigReader
import org.yaml.snakeyaml.nodes.MappingNode

class SignatureConfigReader(val fileResolver: FileResolver) : YamlConfigReader<SignatureConfig>() {
    override fun read(input: MappingNode?): SignatureConfig? {
        return getMapping(input, "signed-response")?.let { node ->
            val config = SignatureConfig()
            getValueAsBool(node, "enabled")?.let {
                config.enabled = it
            }
            if (config.enabled) {
                getValueAsString(node, "algorithm")?.let {
                    config.algorithm = SignatureConfig.algorithmOfString(it)
                }
                getValueAsString(node, "private-key")?.let {
                    val key = fileResolver.resolve(it)
                    config.privateKey = key.absolutePath
                }
            }
            if (config.enabled && config.privateKey == null) {
                throw IllegalStateException("Path to a private key (`signature.private-key`) is required when Response signature is enabled.")
            }
            config
        }
    }
}
