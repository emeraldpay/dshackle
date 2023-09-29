package io.emeraldpay.dshackle.upstream.grpc.auth

import io.emeraldpay.dshackle.auth.processor.SESSION_ID
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall
import io.grpc.Metadata
import io.grpc.MethodDescriptor

class ClientAuthenticationInterceptor(
    private val upstreamId: String,
    private val grpcAuthContext: GrpcAuthContext,
) : ClientInterceptor {

    companion object {
        private val AUTHORIZATION_HEADER: Metadata.Key<String> =
            Metadata.Key.of(SESSION_ID, Metadata.ASCII_STRING_MARSHALLER)
    }

    override fun <ReqT, RespT> interceptCall(
        method: MethodDescriptor<ReqT, RespT>,
        callOptions: CallOptions,
        next: Channel,
    ): ClientCall<ReqT, RespT> =
        object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            override fun start(responseListener: Listener<RespT>, headers: Metadata) {
                grpcAuthContext.getToken(upstreamId)?.let {
                    headers.put(AUTHORIZATION_HEADER, it)
                }
                super.start(responseListener, headers)
            }
        }
}
