package io.emeraldpay.dshackle.config.spans

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import org.springframework.cloud.sleuth.Tracer

class ServerSpansInterceptor(
    private val tracer: Tracer,
    private val providerSpanHandler: ProviderSpanHandler
) : ServerInterceptor {
    override fun <ReqT : Any?, RespT : Any?> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        val serverCall = if (call.methodDescriptor.fullMethodName == "emerald.Blockchain/NativeCall") {
            SpanServerCall(call)
        } else {
            call
        }

        return next.startCall(serverCall, headers)
    }

    private inner class SpanServerCall<ReqT, RespT>(
        private val call: ServerCall<ReqT, RespT>
    ) : SimpleForwardingServerCall<ReqT, RespT>(call) {
        override fun sendHeaders(headers: Metadata) {
            tracer.currentSpan()?.let {
                val parentId = it.context().parentId()
                if (parentId != null) {
                    val spans = providerSpanHandler.getErrorSpans(it.context().spanId(), it)
                    if (spans.isNotBlank()) {
                        headers.put(Metadata.Key.of(SPAN_HEADER, Metadata.ASCII_STRING_MARSHALLER), spans)
                    }
                }
                call.sendHeaders(headers)
            }
        }
    }
}
