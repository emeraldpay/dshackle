package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.foundation.YamlConfigReader
import org.slf4j.LoggerFactory
import org.springframework.util.ResourceUtils
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.FileNotFoundException

class AuthorizationConfigReader : YamlConfigReader<AuthorizationConfig>() {

    companion object {
        private val log = LoggerFactory.getLogger(AuthorizationConfigReader::class.java)
    }

    override fun read(input: MappingNode?): AuthorizationConfig {
        val auth = getMapping(input, "auth")
        if (auth == null) {
            log.warn("Authorization is not using")
            return AuthorizationConfig.default()
        }

        val enabled = getValueAsBool(auth, "enabled")
        if (enabled == null || !enabled) {
            log.warn("Authorization is not enabled")
            return AuthorizationConfig.default()
        }

        val publicKeyOwner = getValueAsString(auth, "publicKeyOwner")
            ?: throw IllegalStateException("Public key owner in not specified")

        val authServer = getMapping(auth, "server")
            ?.run {
                val keyPair = getMapping(this, "keys")
                    ?: throw IllegalStateException("Auth keys is not specified")
                val privateKey = getValueAsString(keyPair, "provider-private-key")
                    ?: throw IllegalStateException("Private key in not specified")
                val publicKey = getValueAsString(keyPair, "external-public-key")
                    ?: throw IllegalStateException("External key in not specified")

                if (fileNotExists(privateKey)) {
                    throw IllegalStateException("There is no such file: $privateKey")
                }
                if (fileNotExists(publicKey)) {
                    throw IllegalStateException("There is no such file: $publicKey")
                }
                AuthorizationConfig.ServerConfig(privateKey, publicKey)
            }

        val authClient = getMapping(auth, "client")
            ?.run {
                AuthorizationConfig.ClientConfig(getValueAsString(this, "private-key")!!)
            }

        if (authClient == null && authServer == null) {
            throw IllegalStateException("Token auth server settings are not specified")
        }

        return AuthorizationConfig(
            enabled,
            publicKeyOwner,
            authServer ?: AuthorizationConfig.ServerConfig.default(),
            authClient ?: AuthorizationConfig.ClientConfig.default(),
        )
    }

    private fun fileNotExists(path: String): Boolean {
        return try {
            !ResourceUtils.getFile(path).exists()
        } catch (e: FileNotFoundException) {
            true
        }
    }
}
