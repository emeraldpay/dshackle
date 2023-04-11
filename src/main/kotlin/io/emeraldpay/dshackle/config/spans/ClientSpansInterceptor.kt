package io.emeraldpay.dshackle.config.spans

import brave.Tracer
import brave.handler.MutableSpan
import brave.handler.SpanHandler
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc.ForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.Metadata.ASCII_STRING_MARSHALLER
import io.grpc.MethodDescriptor
import org.springframework.beans.factory.annotation.Qualifier

class ClientSpansInterceptor(
    private val zipkinSpanHandler: SpanHandler,
    private val tracer: Tracer,
    @Qualifier("spanMapper")
    private val spanMapper: ObjectMapper
) : ClientInterceptor {

    override fun <ReqT : Any?, RespT : Any?> interceptCall(
        method: MethodDescriptor<ReqT, RespT>,
        callOptions: CallOptions,
        next: Channel
    ): ClientCall<ReqT, RespT> {
        return if (method.fullMethodName == "emerald.Blockchain/NativeCall") {
            SpanClientCall(next.newCall(method, callOptions))
        } else {
            next.newCall(method, callOptions)
        }
    }

    private inner class SpanClientCall<ReqT, RespT>(
        delegate: ClientCall<ReqT, RespT>?
    ) : SimpleForwardingClientCall<ReqT, RespT>(delegate) {
        override fun start(responseListener: Listener<RespT>, headers: Metadata) {
            val spanResponseListener = SpanClientCallListener(responseListener)
            super.start(spanResponseListener, headers)
        }
    }

    private inner class SpanClientCallListener<RespT>(
        delegate: ClientCall.Listener<RespT>,
    ) : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(delegate) {
        override fun onHeaders(headers: Metadata) {
            headers[Metadata.Key.of(SPAN_HEADER, ASCII_STRING_MARSHALLER)]
                ?.takeIf { it.isNotBlank() }
                ?.let {
                    val spansFromProvider = spanMapper.readValue<List<MutableSpan>>(it)
                    spansFromProvider.forEach { span ->
                        zipkinSpanHandler.end(tracer.currentSpan().context(), span, SpanHandler.Cause.FINISHED)
                    }
                }
            super.onHeaders(headers)
        }
    }
}
