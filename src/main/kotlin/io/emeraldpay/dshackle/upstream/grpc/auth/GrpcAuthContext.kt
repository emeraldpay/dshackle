package io.emeraldpay.dshackle.upstream.grpc.auth

import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class GrpcAuthContext {
    private val sessions = ConcurrentHashMap<String, String>()

    fun putTokenInContext(upstreamId: String, sessionId: String) {
        sessions[upstreamId] = sessionId
    }

    fun removeToken(upstreamId: String) {
        sessions.remove(upstreamId)
    }

    fun containsToken(upstreamId: String) = sessions.containsKey(upstreamId)

    fun getToken(upstreamId: String) = sessions[upstreamId]
}
