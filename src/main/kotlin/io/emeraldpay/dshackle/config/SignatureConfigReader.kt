package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.FileResolver
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream

class SignatureConfigReader(
    val fileResolver: FileResolver,
) : YamlConfigReader(),
    ConfigReader<SignatureConfig> {
    companion object {
        private val log = LoggerFactory.getLogger(SignatureConfig::class.java)
    }

    fun read(input: InputStream): SignatureConfig? {
        val configNode = readNode(input)
        return read(configNode)
    }

    override fun read(input: MappingNode?): SignatureConfig? =
        getMapping(input, "signed-response")?.let { node ->
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
                throw IllegalStateException(
                    "Path to a private key (`signature.private-key`) is required when Response signature is enabled.",
                )
            }
            config
        }
}
