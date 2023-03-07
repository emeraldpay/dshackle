package io.emeraldpay.dshackle.test

import brave.Tracer
import org.springframework.cloud.sleuth.CurrentTraceContext
import org.springframework.cloud.sleuth.Span
import org.springframework.cloud.sleuth.TraceContext
import org.springframework.cloud.sleuth.brave.bridge.BraveBaggageManager
import org.springframework.cloud.sleuth.brave.bridge.BraveSpan
import org.springframework.cloud.sleuth.brave.bridge.BraveTracer
import spock.mock.DetachedMockFactory

class TracerMock extends BraveTracer {
    private final spanMock = new SpanMock(null)
    private static final mockFactory = new DetachedMockFactory()

    TracerMock(Tracer tracer, CurrentTraceContext context, BraveBaggageManager braveBaggageManager) {
        super(tracer, context, braveBaggageManager)
    }

    @Override
    Span nextSpan(Span parent) {
        return spanMock
    }

    @Override
    Span currentSpan() {
        return spanMock
    }

    @Override
    SpanInScope withSpan(Span span) {
        return mockFactory.Stub(SpanInScope)
    }

    private static class SpanMock extends BraveSpan {
        SpanMock(brave.Span delegate) {
            super(delegate)
        }

        @Override
        Span start() {
            return this
        }

        @Override
        Span name(String name) {
            return this
        }

        @Override
        Span tag(String key, String value) {
            return this
        }

        @Override
        void end() {
        }

        @Override
        TraceContext context() {
            return mockFactory.Stub(TraceContext)
        }
    }
}
