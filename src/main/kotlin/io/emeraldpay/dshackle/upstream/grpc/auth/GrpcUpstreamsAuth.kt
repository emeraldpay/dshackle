package io.emeraldpay.dshackle.upstream.grpc.auth

import com.auth0.jwt.JWT
import com.auth0.jwt.JWTVerifier
import com.auth0.jwt.algorithms.Algorithm
import io.emeraldpay.api.proto.AuthOuterClass
import io.emeraldpay.api.proto.ReactorAuthGrpc.ReactorAuthStub
import io.emeraldpay.dshackle.auth.processor.AuthVersion
import io.emeraldpay.dshackle.auth.processor.SESSION_ID
import io.emeraldpay.dshackle.auth.processor.VERSION
import io.emeraldpay.dshackle.auth.service.RsaKeyReader
import io.emeraldpay.dshackle.config.AuthorizationConfig
import io.grpc.Status
import io.grpc.StatusRuntimeException
import reactor.core.publisher.Mono
import java.security.interfaces.RSAPrivateKey
import java.security.interfaces.RSAPublicKey
import java.time.Instant

class AuthException(message: String) : RuntimeException(message)

class GrpcUpstreamsAuth(
    private val authClient: ReactorAuthStub,
    private val authorizationConfig: AuthorizationConfig,
    private val grpcAuthContext: GrpcAuthContext,
    publicKeyPath: String
) {
    private val rsaKeyReader = RsaKeyReader()
    private val keys = rsaKeyReader.getKeyPair(authorizationConfig.clientConfig.privateKeyPath, publicKeyPath)

    fun auth(providerId: String): Mono<AuthResult> {
        return authClient.authenticate(
            AuthOuterClass.AuthRequest.newBuilder()
                .setToken(generateToken())
                .build()
        ).map {
            verify(it.providerToken, providerId)
        }.onErrorResume {
            if (it is StatusRuntimeException && it.status.code == Status.Code.UNIMPLEMENTED) {
                Mono.just(AuthResult(true))
                    .also { grpcAuthContext.putTokenInContext(providerId, SESSION_ID) }
            } else {
                Mono.just(AuthResult(false, "Error during auth - ${it.message}"))
            }
        }
    }

    private fun generateToken(): String {
        return JWT.create()
            .withIssuedAt(Instant.now())
            .withIssuer(authorizationConfig.publicKeyOwner)
            .withClaim(VERSION, AuthVersion.V1.toString())
            .sign(Algorithm.RSA256(keys.providerPrivateKey as RSAPrivateKey))
    }

    private fun verify(token: String, providerId: String): AuthResult {
        val verifier: JWTVerifier = JWT
            .require(Algorithm.RSA256(keys.externalPublicKey as RSAPublicKey, null))
            .withClaim(SESSION_ID) { claim, _ -> !claim.isMissing }
            .build()
        val decodedToken = verifier.verify(token)
        grpcAuthContext.putTokenInContext(providerId, decodedToken.getClaim(SESSION_ID).asString())

        return AuthResult(true)
    }

    data class AuthResult(
        val passed: Boolean,
        val cause: String? = null
    )
}
