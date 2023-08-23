package io.emeraldpay.dshackle.auth

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class AuthContext {

    companion object {
        val sessions = ConcurrentHashMap<String, TokenWrapper>()

        fun putTokenInContext(tokenWrapper: TokenWrapper) {
            sessions[tokenWrapper.sessionId] = tokenWrapper
        }

        fun removeToken(sessionId: String) {
            sessions.remove(sessionId)
        }
    }

    data class TokenWrapper(
        val token: String,
        val issuedAt: Instant,
        val sessionId: String
    )
}
