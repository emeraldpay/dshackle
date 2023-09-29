package io.emeraldpay.dshackle.config

data class AuthorizationConfig(
    val enabled: Boolean,
    val publicKeyOwner: String,
    val serverConfig: ServerConfig,
    val clientConfig: ClientConfig,
) {

    fun hasServerConfig() = serverConfig != ServerConfig.default()

    companion object {
        @JvmStatic
        fun default() = AuthorizationConfig(
            false,
            "",
            ServerConfig.default(),
            ClientConfig.default(),
        )
    }

    data class ServerConfig(
        val providerPrivateKeyPath: String,
        val externalPublicKeyPath: String,
    ) {
        companion object {
            @JvmStatic
            fun default() = ServerConfig("", "")
        }
    }

    data class ClientConfig(
        val privateKeyPath: String,
    ) {
        companion object {
            @JvmStatic
            fun default() = ClientConfig("")
        }
    }
}
