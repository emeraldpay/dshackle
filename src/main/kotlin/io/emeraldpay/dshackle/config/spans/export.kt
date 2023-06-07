package io.emeraldpay.dshackle.config.spans

import brave.handler.MutableSpan
import io.emeraldpay.dshackle.commons.SPAN_ERROR
import io.emeraldpay.dshackle.commons.SPAN_NO_RESPONSE_MESSAGE
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

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

@Component
class LongResponseSpanExportable(
    @Value("\${spans.collect.provider.long-span-threshold}")
    private val longSpanThreshold: Long? = null
) : SpanExportable {

    override fun isExportable(span: MutableSpan): Boolean {
        val duration = span.finishTimestamp() - span.startTimestamp()
        return TimeUnit.SECONDS.convert(duration, TimeUnit.MICROSECONDS) >= longSpanThreshold!!
    }
}
