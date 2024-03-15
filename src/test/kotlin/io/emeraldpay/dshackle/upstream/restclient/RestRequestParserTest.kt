package io.emeraldpay.dshackle.upstream.restclient

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

class RestRequestParserTest {

    @Test
    fun `transform query params into string`() {
        val queryParams = mapOf(
            "first" to "coolParam",
            "second" to "moreCoolParam",
        )

        val result = RestRequestParser.transformQueryParams(queryParams)

        assertEquals("?first=coolParam&second=moreCoolParam", result)
    }

    @Test
    fun `transform query params into empty string`() {
        val result = RestRequestParser.transformQueryParams(emptyMap())

        assertEquals("", result)
    }

    @ParameterizedTest
    @MethodSource("data")
    fun `transform path params into path with these params`(path: String, params: List<String>, expected: String) {
        val result = RestRequestParser.transformPathParams(path, params)

        assertEquals(expected, result)
    }

    companion object {
        @JvmStatic
        fun data(): List<Arguments> {
            return listOf(
                Arguments.of("/eth/v1/blocks/*/state/*/root", listOf("123", "678"), "/eth/v1/blocks/123/state/678/root"),
                Arguments.of("/eth/v1/blocks/*/state/*", listOf("123", "678"), "/eth/v1/blocks/123/state/678"),
                Arguments.of("/eth/v1/blocks", emptyList<String>(), "/eth/v1/blocks"),
                Arguments.of("/eth/v1/blocks/*", listOf("123", "678"), "/eth/v1/blocks/123"),
                Arguments.of("/eth/v1/blocks", listOf("123", "678"), "/eth/v1/blocks"),
            )
        }
    }
}
