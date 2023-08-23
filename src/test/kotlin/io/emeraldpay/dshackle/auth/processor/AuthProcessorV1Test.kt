package io.emeraldpay.dshackle.auth.processor

import com.auth0.jwt.JWT
import com.auth0.jwt.JWTVerifier
import com.auth0.jwt.RegisteredClaims
import com.auth0.jwt.algorithms.Algorithm
import io.emeraldpay.dshackle.auth.service.RsaKeyReader
import io.emeraldpay.dshackle.config.AuthorizationConfig
import io.grpc.StatusException
import org.bouncycastle.openssl.PEMParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.util.ResourceUtils
import java.io.StringReader
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyFactory
import java.security.PublicKey
import java.security.interfaces.RSAPublicKey
import java.security.spec.X509EncodedKeySpec

class AuthProcessorV1Test {
    private val processor = AuthProcessorV1(AuthorizationConfig(true, "drpc", "", ""))
    private val rsaKeyReader = RsaKeyReader()
    private val privProviderPath = ResourceUtils.getFile("classpath:keys/priv.p8.key").path
    private val publicDrpcPath = ResourceUtils.getFile("classpath:keys/public-drpc.pem").path
    private val token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJkcnBjIiwiaWF0IjoxNjkyMTg1OTMxLCJ2ZXJzaW9uI" +
        "joiVjEifQ.BZILN0GQ7JzXGFz-GZIbFTT9E5L-miB4Nga0v4o_cQThk8gbDelBRzEfdsqxCq_ppPr3v_Own8M-vR9yQElx5nEdlI4xe5QAMdIvr3g" +
        "12fMckydX9IsW4sVQ1kJJY8RrHb-WL-uI0WSWqoMSwf-Psb-UyiEHAjc3oK7fA72lBaGT4waPHOxRBPvezwg7N934vCZvZMAftFfVgmeEtbCeD7bF" +
        "umEr0uEmkIKPTg4QwP-VMvqoLBYpMiJVzP_Ipg_wRHJ7fUN0BGEPjjMvhQ_6TWByiQUBz1kTMd0Ebf_kEuXFQeiwA-FXHJpWczzh66CbbmmWAWsi" +
        "ehKw3KPZeBj0oQ"
    private val keyPair = rsaKeyReader.getKeyPair(privProviderPath, publicDrpcPath)

    @Test
    fun `verify tokens is successful`() {
        val publicProviderPath = ResourceUtils.getFile("classpath:keys/public.pem").path

        val providerToken = processor.process(keyPair, token).token
        val verifier: JWTVerifier = JWT.require(Algorithm.RSA256(generatePublicKey(publicProviderPath) as RSAPublicKey, null))
            .withClaim(VERSION, "V1")
            .build()
        val decodedToken = verifier.verify(providerToken)
        assertTrue(!decodedToken.getClaim(SESSION_ID).isMissing)
        assertTrue(!decodedToken.getClaim(RegisteredClaims.ISSUED_AT).isMissing)
    }

    @Test
    fun `verify token is failed by wrong key`() {
        val publicProviderPath = ResourceUtils.getFile("classpath:keys/wrong-public.pem").path
        val keyPair = rsaKeyReader.getKeyPair(privProviderPath, publicProviderPath)

        val e = assertThrows(StatusException::class.java) { processor.process(keyPair, token) }
        assertEquals(
            "INVALID_ARGUMENT: Invalid token: The Token's Signature resulted invalid when verified using the Algorithm: SHA256withRSA",
            e.message
        )
    }

    @Test
    fun `verify token is failed if no issuer`() {
        val invalidToken = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJkcnBjY2NjIiwiaWF0IjoxNjkyMTg3NDMwLCJ2ZXJzaW" +
            "9uIjoiVjEifQ.J1WJ1GvjNJ9JQiMK0bwvtGX1o9P93F5-921myIx3TMa2X48qIG0GVEcoMgv01ca-_aisW-Amk27ygI09dPKE__Ijr6JhZ" +
            "fDVNkw_ZArtQTcjhhJiCl3pqsouOlojc8EolpYUmyOefqemqycG0B84ibKAWTdXOtjibt1P5szWjIIV9yOYV7lTJkC0B5swcjjaMvTEPU7y" +
            "ZJhg_wvCvT67yFM1K_Wnhys3-j-Xv1Y2wOkxNt4i5LKFDtMZml5eTIEscDpjp5ARjaSTW_Rs1Eixqltx_wz1ALiS0QXOJpX7pVMJjRcth4Nu" +
            "R87ej434XoHZWqDmvOEM6M855WeHaO761A"

        val e = assertThrows(StatusException::class.java) { processor.process(keyPair, invalidToken) }
        assertEquals(
            "INVALID_ARGUMENT: Invalid token: The Claim 'iss' value doesn't match the required issuer.",
            e.message
        )
    }

    private fun generatePublicKey(path: String): PublicKey {
        val publicKeyReader = StringReader(Files.readString(Paths.get(path)))

        val publicPem = PEMParser(publicKeyReader).readPemObject()

        val publicKeySpec = X509EncodedKeySpec(publicPem.content)

        return KeyFactory.getInstance("RSA").generatePublic(publicKeySpec)
    }
}
