package io.emeraldpay.dshackle.auth.processor

import com.auth0.jwt.JWT
import io.emeraldpay.dshackle.config.AuthorizationConfig
import io.grpc.StatusException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class AuthProcessorResolverTest {
    private val authProcessorV1 = AuthProcessorV1(AuthorizationConfig.default())
    private val authProcessorResolver = AuthProcessorResolver(authProcessorV1)

    @Test
    fun `get processor of V1 version`() {
        val token = JWT.decode(
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJkcnBjIiwiaWF0IjoxNjkyMTg1OTMxLCJ2ZXJzaW9uI" +
                "joiVjEifQ.BZILN0GQ7JzXGFz-GZIbFTT9E5L-miB4Nga0v4o_cQThk8gbDelBRzEfdsqxCq_ppPr3v_Own8M-vR9yQElx5nEdlI4xe5QAMdIvr3g" +
                "12fMckydX9IsW4sVQ1kJJY8RrHb-WL-uI0WSWqoMSwf-Psb-UyiEHAjc3oK7fA72lBaGT4waPHOxRBPvezwg7N934vCZvZMAftFfVgmeEtbCeD7bF" +
                "umEr0uEmkIKPTg4QwP-VMvqoLBYpMiJVzP_Ipg_wRHJ7fUN0BGEPjjMvhQ_6TWByiQUBz1kTMd0Ebf_kEuXFQeiwA-FXHJpWczzh66CbbmmWAWsi" +
                "ehKw3KPZeBj0oQ"
        )
        val processor = authProcessorResolver.getAuthProcessor(token)

        assertTrue(processor is AuthProcessorV1)
    }

    @Test
    fun `failed if no version is in a token`() {
        val token = JWT.decode(
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJkcnBjIiwiaWF0IjoxNjkyMTg5NTUzfQ.sQ1Q3DFC7kOlHWdnWaxv2F" +
                "vhGso6ajJQVWtrE1hYmL_AIH_Lz8LNpqFShlL2itEGfmyRz5-Vrbdf-yWjeyLyNDlf7pay2lNE5Pvm2-EtQd8GUYcCFFIz7Sxc2Iphe2" +
                "YIx6kBwSlaR0RXBcmUlOtKrnON0bBNzSojBmtCT7-j4hTpoKhYr04Fr9EJWHfw7grVZjU8rEizAX_SR3ZNoufjK_pZaIyI9qUKVPYSepP" +
                "lXtQzVjA80qSeYpkeFCOLwlQD_yTArDNWlwe7-CthtBOAtctoTMwyudfJezT2ilXrigzbauzU5BEi1cNxacHpjNuXhyY0TiacJGugWfRgaaGy6g"
        )

        val e = assertThrows(StatusException::class.java) { authProcessorResolver.getAuthProcessor(token) }
        assertEquals("INVALID_ARGUMENT: Version is not specified in the token", e.message)
    }

    @Test
    fun `failed if the wrong version is specified`() {
        val token = JWT.decode(
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJkcnBjIiwiaWF0IjoxNjkyMTg5OTI3LCJ2ZXJzaW9uIjoiVjIifQ.pT" +
                "mG3Z-h9P1DlocTKZ3BpAKNdsvRGjXo71I0GUpUemAnauUaH-OgxUOHlNcZ2uB4f1poEWsbpExeuZ15YAiBf2WBCEV6J6xH1u0cPC1O-8" +
                "hHNb46166ngzo-BtZ7Rn7FeVEayyICslc9iXM5GvoFJyJIdn9uLMzalyDJq2bUmIelym4edkQ3ybF-pqf8garuVVErnAsbOFXbYQlfO5ZJ" +
                "4zq6PfJo7QSkreMzQ4tK_-JJIGG-EK1bQjsAD7JiXipY2cJY17VlHuavI0DfcJlOe-QggbTH63rxL6JvXCyFux7gI7zdqSBP1fNDJNzTLA" +
                "cnR7jCN0kMIa8urQgdePZyRg"
        )

        val e = assertThrows(StatusException::class.java) { authProcessorResolver.getAuthProcessor(token) }
        assertEquals("INVALID_ARGUMENT: Unsupported auth version V2", e.message)
    }
}
