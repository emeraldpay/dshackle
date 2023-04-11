package io.emeraldpay.dshackle.config.spans

import brave.handler.MutableSpan
import brave.handler.SpanHandler
import brave.propagation.TraceContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.benmanes.caffeine.cache.Caffeine
import io.emeraldpay.dshackle.commons.SPAN_ERROR
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.cloud.sleuth.Span
import java.time.Duration

class ErrorSpanHandler(
    @Qualifier("spanMapper")
    private val spanMapper: ObjectMapper,
) : SpanHandler() {
    private val spans = Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofMinutes(5))
        .build<String, MutableList<MutableSpan>>()

    override fun end(context: TraceContext, span: MutableSpan, cause: Cause): Boolean {
        if (span.traceId().length > 20 && span.parentId() != null) {
            val spanList = spans.asMap().computeIfAbsent(span.parentId()) { mutableListOf() }
            spanList.add(span)
        }
        return super.end(context, span, cause)
    }

    fun getErrorSpans(spanId: String, currentSpan: Span): String {
        val spansInfo = SpansInfo()

        enrichErrorSpans(spanId, spansInfo)
        currentSpan.end()
        currentSpan.context().parentId()?.let {
            spans.getIfPresent(it)?.let { mutableSpans ->
                if (mutableSpans.isNotEmpty()) {
                    spansInfo.spans.add(mutableSpans[0])
                }
            }
        }

        spansInfo.spans
            .map { it.parentId() }
            .forEach {
                if (it != null) {
                    spans.invalidate(it)
                }
            }

        return if (spansInfo.hasError) {
            spanMapper.writeValueAsString(spansInfo.spans)
        } else {
            ""
        }
    }

    private fun enrichErrorSpans(spanId: String, spansInfo: SpansInfo) {
        val currentSpans: List<MutableSpan>? = spans.getIfPresent(spanId)

        currentSpans?.forEach {
            spansInfo.spans.add(it)
            if (it.tags().containsKey(SPAN_ERROR)) {
                spansInfo.hasError = true
            }
            if (spanId != it.id()) {
                enrichErrorSpans(it.id(), spansInfo)
            }
        }
    }

    private data class SpansInfo(
        var hasError: Boolean = false,
        val spans: MutableList<MutableSpan> = mutableListOf()
    )
}
