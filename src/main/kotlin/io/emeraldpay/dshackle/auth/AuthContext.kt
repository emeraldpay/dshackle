package io.emeraldpay.dshackle.auth

import com.github.benmanes.caffeine.cache.Caffeine
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

@Component
class AuthContext {
    private val sessions = Caffeine.newBuilder()
        .expireAfterAccess(Duration.ofDays(1))
        .build<String, Boolean>()

    fun putSessionInContext(tokenWrapper: TokenWrapper) {
        sessions.put(tokenWrapper.sessionId, true)
    }

    fun containsSession(sessionId: String): Boolean {
        return sessions.asMap()[sessionId] != null
    }

    data class TokenWrapper(
        val token: String,
        val issuedAt: Instant,
        val sessionId: String
    )
}
