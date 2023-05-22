package io.emeraldpay.dshackle.config.spans

import brave.handler.MutableSpan
import io.emeraldpay.dshackle.commons.SPAN_ERROR
import io.emeraldpay.dshackle.commons.SPAN_NO_RESPONSE_MESSAGE
import org.springframework.stereotype.Component

interface SpanExportable {
    fun isExportable(span: MutableSpan): Boolean
}

@Component
class ErrorSpanExportable : SpanExportable {
    override fun isExportable(span: MutableSpan): Boolean = span.tags().containsKey(SPAN_ERROR)
}

@Component
class NoResponseSpanExportable : SpanExportable {
    override fun isExportable(span: MutableSpan): Boolean = span.tags().containsKey(SPAN_NO_RESPONSE_MESSAGE)
}
