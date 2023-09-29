package io.emeraldpay.dshackle.auth

import io.emeraldpay.dshackle.auth.processor.SESSION_ID
import io.grpc.Metadata
import io.grpc.Metadata.ASCII_STRING_MARSHALLER
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import org.springframework.stereotype.Component

const val AUTH_METHOD_NAME = "emerald.Auth/Authenticate"
const val REFLECT_METHOD_NAME = "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"

@Component
class AuthInterceptor(
    private val authContext: AuthContext,
) : ServerInterceptor {
    private val specialMethods = setOf(AUTH_METHOD_NAME, REFLECT_METHOD_NAME)

    override fun <ReqT : Any, RespT : Any> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
        val sessionId = headers.get(
            Metadata.Key.of(SESSION_ID, ASCII_STRING_MARSHALLER),
        )
        val isOrdinaryMethod = !specialMethods.contains(call.methodDescriptor.fullMethodName)

        if (isOrdinaryMethod && (sessionId == null || !authContext.containsSession(sessionId))) {
            val cause = if (sessionId == null) "sessionId is not passed" else "Session $sessionId does not exist"
            throw Status.UNAUTHENTICATED
                .withDescription(cause)
                .asException()
        }

        return next.startCall(call, headers)
    }
}
