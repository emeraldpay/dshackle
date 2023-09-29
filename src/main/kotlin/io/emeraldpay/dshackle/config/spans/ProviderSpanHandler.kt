package io.emeraldpay.dshackle.config.spans

import brave.handler.MutableSpan
import brave.handler.SpanHandler
import brave.propagation.TraceContext
import com.github.benmanes.caffeine.cache.Caffeine
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.time.Duration

@Component
@ConditionalOnProperty(value = ["spring.zipkin.enabled"], havingValue = "true")
class ProviderSpanHandler(
    private val spanExportableList: List<SpanExportable>,
    private val zipkinSpanHandler: SpanHandler,
) : SpanHandler() {
    private val log = LoggerFactory.getLogger(ProviderSpanHandler::class.java)

    private val spans = Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofMinutes(2))
        .build<String, MutableList<MutableSpan>>()

    override fun end(context: TraceContext, span: MutableSpan, cause: Cause): Boolean {
        if (span.traceId().length > 20 && cause == Cause.FINISHED) {
            val key = span.parentId() ?: span.id()
            val spanList = spans.asMap().computeIfAbsent(key) { mutableListOf() }
            spanList.add(span)
        }
        return false
    }

    fun sendSpans(traceContext: TraceContext) {
        try {
            sendSpansInternal(traceContext)
        } catch (e: Exception) {
            log.warn("Error while handling and sending spans - ${e.message}")
        }
    }

    private fun sendSpansInternal(traceContext: TraceContext) {
        val spansInfo = SpansInfo()

        processSpans(traceContext.spanIdString(), spansInfo)

        spansInfo.spans
            .map { it.parentId() }
            .forEach {
                if (it != null) {
                    spans.invalidate(it)
                }
            }

        if (spansInfo.exportable) {
            spansInfo.spans.forEach {
                zipkinSpanHandler.end(traceContext, it, Cause.FINISHED)
            }
        }
    }

    private fun processSpans(spanId: String, spansInfo: SpansInfo) {
        val currentSpans: List<MutableSpan>? = spans.getIfPresent(spanId)

        currentSpans?.forEach {
            processSpanInfo(it, spansInfo)
            if (spanId != it.id()) {
                processSpans(it.id(), spansInfo)
            }
        }
    }

    private fun processSpanInfo(span: MutableSpan, spansInfo: SpansInfo) {
        spansInfo.spans.add(span)
        if (spanExportableList.any { it.isExportable(span) }) {
            spansInfo.exportable = true
        }
    }

    private data class SpansInfo(
        var exportable: Boolean = false,
        val spans: MutableList<MutableSpan> = mutableListOf(),
    )
}
