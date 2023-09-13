package io.emeraldpay.dshackle.auth.service

import com.auth0.jwt.JWT
import io.emeraldpay.dshackle.auth.AuthContext
import io.emeraldpay.dshackle.auth.processor.AuthProcessorResolver
import io.emeraldpay.dshackle.config.AuthorizationConfig
import io.grpc.Status
import org.springframework.stereotype.Service

@Service
class AuthService(
    private val authorizationConfig: AuthorizationConfig,
    private val rsaKeyReader: KeyReader,
    private val authProcessorResolver: AuthProcessorResolver
) {

    fun authenticate(token: String): String {
        if (!authorizationConfig.enabled) {
            throw Status.UNIMPLEMENTED
                .withDescription("Authentication process is not enabled")
                .asException()
        }

        val keys = rsaKeyReader.getKeyPair(
            authorizationConfig.serverConfig.providerPrivateKeyPath,
            authorizationConfig.serverConfig.externalPublicKeyPath
        )
        val decodedJwt = JWT.decode(token)

        return authProcessorResolver
            .getAuthProcessor(decodedJwt)
            .process(keys, token)
            .run {
                AuthContext.putTokenInContext(this)
                this.token
            }
    }
}
