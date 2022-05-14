package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.FileResolver
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream
import java.security.spec.PKCS8EncodedKeySpec
import java.security.KeyFactory;

class SignatureConfigReader(val fileResolver: FileResolver) : YamlConfigReader(), ConfigReader<SignatureConfig> {
    companion object {
        private val log = LoggerFactory.getLogger(SignatureConfig::class.java)
    }

    fun read(input: InputStream): SignatureConfig? {
        val configNode = readNode(input)
        return read(configNode)
    }

    override fun read(input: MappingNode?): SignatureConfig? {
        return getMapping(input, "signature")?.let { node ->
            val config = SignatureConfig()
            config.enabled = getValueAsBool(node, "enabled") ?: config.enabled
            config.signScheme = getValueAsString(node, "scheme") ?: config.signScheme
            config.algorithm = getValueAsString(node, "algorithm") ?: config.algorithm

            config.privateKey = getValueAsString(node, "privateKey").let {
                if (it == null) {
                    return null
                }
                val key = fileResolver.resolve(it).readBytes()
                val keyFactory = KeyFactory.getInstance(config.algorithm);
                val keySpec = PKCS8EncodedKeySpec(key)
                keyFactory.generatePrivate(keySpec)
            }
            config
        }
    }
}