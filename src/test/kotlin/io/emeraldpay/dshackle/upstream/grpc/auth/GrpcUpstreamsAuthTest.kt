package io.emeraldpay.dshackle.upstream.grpc.auth

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import io.emeraldpay.api.proto.AuthOuterClass
import io.emeraldpay.api.proto.AuthOuterClass.AuthRequest
import io.emeraldpay.api.proto.ReactorAuthGrpc.ReactorAuthStub
import io.emeraldpay.dshackle.auth.processor.SESSION_ID
import io.emeraldpay.dshackle.config.AuthorizationConfig
import org.bouncycastle.openssl.PEMParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.any
import org.mockito.Mockito.`when`
import org.springframework.util.ResourceUtils
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.io.StringReader
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.interfaces.RSAPrivateKey
import java.security.spec.PKCS8EncodedKeySpec
import java.time.Duration
import java.util.UUID

class GrpcUpstreamsAuthTest {
    private val privateKeyPath = ResourceUtils.getFile("classpath:keys/priv-drpc.p8.key").path
    private val providerPublicKeyPath = ResourceUtils.getFile("classpath:keys/public.pem").path
    private val grpcAuthContext = GrpcAuthContext()
    private val authConfig = AuthorizationConfig(
        true,
        "drpc",
        AuthorizationConfig.ServerConfig.default(),
        AuthorizationConfig.ClientConfig(privateKeyPath),
    )

    private val providerPrivateKeyPath = ResourceUtils.getFile("classpath:keys/priv.p8.key").path

    private val upstreamId = "providerId"

    @BeforeEach
    fun clearSessions() {
        grpcAuthContext.removeToken(upstreamId)
    }

    @Test
    fun `success auth`() {
        val sessionId = UUID.randomUUID().toString()
        val authStub = Mockito.mock(ReactorAuthStub::class.java)
        val grpcAuth = GrpcUpstreamsAuth(authStub, authConfig, grpcAuthContext, providerPublicKeyPath)
        val token = JWT.create()
            .withClaim(SESSION_ID, sessionId)
            .sign(Algorithm.RSA256(generatePrivateKey(providerPrivateKeyPath) as RSAPrivateKey))

        `when`(authStub.authenticate(any(AuthRequest::class.java)))
            .thenReturn(
                Mono.just(
                    AuthOuterClass.AuthResponse.newBuilder()
                        .setProviderToken(token)
                        .build(),
                ),
            )

        val result = grpcAuth.auth(upstreamId)

        StepVerifier.create(result)
            .expectNext(GrpcUpstreamsAuth.AuthResult(true))
            .then {
                assertEquals(sessionId, grpcAuthContext.getToken(upstreamId))
            }
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }

    @Test
    fun `auth is failed`() {
        val providerId = "providerId"
        val sessionId = UUID.randomUUID().toString()
        val authStub = Mockito.mock(ReactorAuthStub::class.java)
        val grpcAuth = GrpcUpstreamsAuth(authStub, authConfig, grpcAuthContext, providerPublicKeyPath)
        val token = JWT.create()
            .withClaim(SESSION_ID, sessionId)
            .sign(Algorithm.RSA256(generatePrivateKey(privateKeyPath) as RSAPrivateKey))

        `when`(authStub.authenticate(any(AuthRequest::class.java)))
            .thenReturn(
                Mono.just(
                    AuthOuterClass.AuthResponse.newBuilder()
                        .setProviderToken(token)
                        .build(),
                ),
            )

        val result = grpcAuth.auth(providerId)

        StepVerifier.create(result)
            .expectNext(
                GrpcUpstreamsAuth.AuthResult(
                    false,
                    "Error during auth - The Token's Signature resulted invalid when verified using the Algorithm: SHA256withRSA",
                ),
            )
            .then {
                assertEquals(null, grpcAuthContext.getToken(upstreamId))
            }
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }

    @Test
    fun `replace sessionId for the same provider`() {
        val providerId = "providerId"
        val sessionId = UUID.randomUUID().toString()
        val sessionId1 = UUID.randomUUID().toString()
        val authStub = Mockito.mock(ReactorAuthStub::class.java)
        val grpcAuth = GrpcUpstreamsAuth(authStub, authConfig, grpcAuthContext, providerPublicKeyPath)
        val token = JWT.create()
            .withClaim(SESSION_ID, sessionId)
            .sign(Algorithm.RSA256(generatePrivateKey(providerPrivateKeyPath) as RSAPrivateKey))
        val token1 = JWT.create()
            .withClaim(SESSION_ID, sessionId1)
            .sign(Algorithm.RSA256(generatePrivateKey(providerPrivateKeyPath) as RSAPrivateKey))

        `when`(authStub.authenticate(any(AuthRequest::class.java)))
            .thenReturn(
                Mono.just(
                    AuthOuterClass.AuthResponse.newBuilder()
                        .setProviderToken(token)
                        .build(),
                ),
            )
            .thenReturn(
                Mono.just(
                    AuthOuterClass.AuthResponse.newBuilder()
                        .setProviderToken(token1)
                        .build(),
                ),
            )

        grpcAuth.auth(providerId).block()
        val result = grpcAuth.auth(providerId)

        StepVerifier.create(result)
            .expectNext(GrpcUpstreamsAuth.AuthResult(true))
            .then {
                assertEquals(sessionId1, grpcAuthContext.getToken(upstreamId))
            }
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }

    private fun generatePrivateKey(path: String): PrivateKey {
        val privateKeyReader = StringReader(Files.readString(Paths.get(path)))
        val privatePem = PEMParser(privateKeyReader).readPemObject()
        val privateKeySpec = PKCS8EncodedKeySpec(privatePem.content)

        return KeyFactory.getInstance("RSA").generatePrivate(privateKeySpec)
    }
}
