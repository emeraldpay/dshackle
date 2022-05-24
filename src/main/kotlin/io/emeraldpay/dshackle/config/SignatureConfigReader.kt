package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.FileResolver
import org.bouncycastle.util.io.pem.PemObject
import org.bouncycastle.util.io.pem.PemReader
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream
import java.security.spec.PKCS8EncodedKeySpec
import java.security.KeyFactory;
import java.security.PrivateKey

class SignatureConfigReader(val fileResolver: FileResolver) : YamlConfigReader(), ConfigReader<SignatureConfig> {
    companion object {
        private val log = LoggerFactory.getLogger(SignatureConfig::class.java)
    }

    fun read(input: InputStream): SignatureConfig? {
        val configNode = readNode(input)
        return read(configNode)
    }

    private fun readKey(algorithm: SignatureConfig.Algorithm, pem: PemObject): PrivateKey {
        val key = when (algorithm) {
            SignatureConfig.Algorithm.ECDSA -> {
                val keyFactory = KeyFactory.getInstance("EC");
                val keySpec = PKCS8EncodedKeySpec(pem.content)
                keyFactory.generatePrivate(keySpec)
            }
        }
        return key
    }

    override fun read(input: MappingNode?): SignatureConfig? {
        return getMapping(input, "signature")?.let { node ->
            val config = SignatureConfig()
            config.enabled = getValueAsBool(node, "enabled") ?: config.enabled
            getValueAsString(node, "algorithm")?.let {
                config.algorithm = SignatureConfig.algorithmOfString(it)
            }
            getValueAsString(node, "privateKey")?.let {
                val key = fileResolver.resolve(it)
                val reader = PemReader(key.reader())
                config.privateKey = readKey(config.algorithm, reader.readPemObject())
            }
            config
        }
    }
}