package io.emeraldpay.dshackle.auth.processor

import com.auth0.jwt.interfaces.DecodedJWT
import io.grpc.Status
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class AuthProcessorResolver(
    private val authProcessorV1: AuthProcessor
) {

    companion object {
        private val log = LoggerFactory.getLogger(AuthProcessorResolver::class.java)
    }

    fun getAuthProcessor(token: DecodedJWT): AuthProcessor {
        val claimVersion = token.getClaim(VERSION)
        if (claimVersion.isMissing) {
            throw Status.INVALID_ARGUMENT
                .withDescription("Version is not specified in the token")
                .asException()
        }

        val version = AuthVersion.getVersion(claimVersion.asString())
        log.info("Using $version of authentication")

        if (version == AuthVersion.V1) {
            return authProcessorV1
        }

        throw Status.INVALID_ARGUMENT
            .withDescription("Unsupported auth version $version")
            .asException()
    }
}
