package io.emeraldpay.dshackle.config.spans

import brave.handler.MutableSpan
import brave.handler.SpanHandler
import brave.propagation.TraceContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

@Component
@ConditionalOnProperty(value = ["spring.zipkin.enabled"], havingValue = "true")
class ProviderSpanHandler(
    @Value("\${spans.collect.collect-only-errors:false}")
    private val onlyErrors: Boolean? = false,
    @Value("\${spans.collect.long-span-threshold}")
    private val longSpanThreshold: Long? = null
) : SpanHandler() {

    override fun end(context: TraceContext?, span: MutableSpan, cause: Cause?): Boolean {
        val duration = TimeUnit.MILLISECONDS.convert(span.finishTimestamp() - span.startTimestamp(), TimeUnit.MICROSECONDS)

        val isError = span.tags().containsKey("error")

        // If onlyErrors is true and there is an error, return true.
        if (onlyErrors == true && isError) {
            return true
        }

        // If onlyErrors is true, there is no error, and the span is long, return true.
        if (onlyErrors == true && !isError && longSpanThreshold != null && duration >= longSpanThreshold) {
            return true
        }

        // If onlyErrors is false, check for the time threshold condition.
        if (onlyErrors == false) {
            // If longSpanThreshold is null, return true.
            if (longSpanThreshold == null) {
                return true
            }
            // If longSpanThreshold is set, only return true if the duration is >= time threshold.
            else if (duration >= longSpanThreshold) {
                return true
            }
        }

        // Return false if none of the above conditions are met.
        return false
    }
}
