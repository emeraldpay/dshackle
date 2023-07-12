package io.emeraldpay.dshackle.config.spans

import brave.handler.MutableSpan
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class ProviderSpanHandlerTest {

    companion object {
        @JvmStatic
        fun data(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    false, null,
                    MutableSpan().apply {
                        startTimestamp(0)
                        finishTimestamp(1000000)
                    },
                    true
                ),
                Arguments.of(
                    false, 1000L,
                    MutableSpan().apply {
                        startTimestamp(0)
                        finishTimestamp(999999)
                    },
                    false
                ),
                Arguments.of(
                    false, 1000L,
                    MutableSpan().apply {
                        startTimestamp(0)
                        finishTimestamp(1000000)
                    },
                    true
                ),
                Arguments.of(
                    true, null,
                    MutableSpan().apply {
                        startTimestamp(0)
                        finishTimestamp(1000000)
                    },
                    false
                ),
                Arguments.of(
                    true, null,
                    MutableSpan().apply {
                        startTimestamp(0)
                        finishTimestamp(1000000)
                        tag("error", "true")
                    },
                    true
                ),
                Arguments.of(
                    false, null,
                    MutableSpan().apply {
                        startTimestamp(0)
                        finishTimestamp(1000000)
                        tag("error", "true")
                    },
                    true
                ),
                Arguments.of(
                    true, 1000L,
                    MutableSpan().apply {
                        startTimestamp(0)
                        finishTimestamp(1000000)
                    },
                    true
                ),
            )
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    fun end(onlyErrors: Boolean?, timeThreshold: Long?, span: MutableSpan, expected: Boolean) {
        val handler = ProviderSpanHandler(onlyErrors, timeThreshold)
        val res = handler.end(null, span, null)
        assertEquals(expected, res)
    }
}
