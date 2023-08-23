package io.emeraldpay.dshackle.config

data class AuthorizationConfig(
    val enabled: Boolean,
    val publicKeyOwner: String,
    val providerPrivateKeyPath: String,
    val externalPublicKeyPath: String
) {

    companion object {
        @JvmStatic
        fun default() = AuthorizationConfig(false, "", "", "")
    }
}
