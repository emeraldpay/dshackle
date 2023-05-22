package io.emeraldpay.dshackle.config.spans

import brave.handler.MutableSpan
import brave.handler.SpanHandler
import brave.propagation.TraceContext
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.commons.SPAN_ERROR
import io.emeraldpay.dshackle.commons.SPAN_NO_RESPONSE_MESSAGE
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import org.springframework.cloud.sleuth.Span
import org.springframework.cloud.sleuth.brave.bridge.BraveTraceContext

class ProviderSpanHandlerTest {
    private val mapper = SpanConfig().spanMapper()
    private val spanExportableList = listOf(ErrorSpanExportable(), NoResponseSpanExportable())
    private val ctx = TraceContext.newBuilder()
        .traceId(1223324)
        .spanId(234235)
        .build()

    @Test
    fun `span with length of traceId less than 20 is not collected`() {
        val spanId = "f7e83f2b69ec684d"
        val currentSpan = Mockito.mock(Span::class.java)
        val handler = spanHandler()

        `when`(currentSpan.context()).thenReturn(BraveTraceContext(ctx))

        handler.end(ctx, span("f7e83f2b69ec684d", spanId), SpanHandler.Cause.FINISHED)

        val result = handler.getErrorSpans(spanId, currentSpan)
        assertEquals("", result)
    }

    @Test
    fun `span without parenId is not collected`() {
        val spanId = "f7e83f2b69ec684d"
        val currentSpan = Mockito.mock(Span::class.java)
        val handler = spanHandler()

        `when`(currentSpan.context()).thenReturn(BraveTraceContext(ctx))

        handler.end(ctx, span("6666632728347823749827349723985", spanId), SpanHandler.Cause.FINISHED)

        val result = handler.getErrorSpans(spanId, currentSpan)
        assertEquals("", result)
    }

    @ParameterizedTest
    @MethodSource("spans")
    fun `span with length of traceId greater than 20 and with parentId is collected`(span: MutableSpan) {
        val currentSpan = Mockito.mock(Span::class.java)
        val handler = spanHandler()

        `when`(currentSpan.context()).thenReturn(BraveTraceContext(ctx))

        handler.end(ctx, span, SpanHandler.Cause.FINISHED)

        val result = handler.getErrorSpans("f7e83f2b69ec682d", currentSpan)
        val collectedSpans = mapper.readValue<List<MutableSpan>>(result)
        assertTrue(collectedSpans.size == 1)
        assertEquals(span, collectedSpans[0])
    }

    @Test
    fun `span without error tag is not collected`() {
        val spanId = "f7e83f2b69ec684d"
        val currentSpan = Mockito.mock(Span::class.java)
        val handler = spanHandler()
        val span = span("6666632728347823749827349723985", spanId)
            .apply {
                parentId("f7e83f2b69ec682d")
                removeTag(SPAN_ERROR)
            }

        `when`(currentSpan.context()).thenReturn(BraveTraceContext(ctx))

        handler.end(ctx, span, SpanHandler.Cause.FINISHED)

        val result = handler.getErrorSpans("f7e83f2b69ec682d", currentSpan)
        assertEquals("", result)
    }

    private fun span(traceId: String, spanId: String) = MutableSpan()
        .apply {
            traceId(traceId)
            id(spanId)
            tag(SPAN_ERROR, "true")
        }

    companion object {
        @JvmStatic
        fun spans() = listOf(
            MutableSpan()
                .apply {
                    traceId("6666632728347823749827349723985")
                    id("f7e83f2b69ec682d")
                    tag(SPAN_ERROR, "true")
                    parentId("f7e83f2b69ec682d")
                },
            MutableSpan()
                .apply {
                    traceId("6666632728347823749827349723985")
                    id("f7e83f2b69ec111d")
                    parentId("f7e83f2b69ec682d")
                    tag(SPAN_NO_RESPONSE_MESSAGE, "noResp")
                }
        )
    }

    private fun spanHandler() = ProviderSpanHandler(mapper, spanExportableList)
}
