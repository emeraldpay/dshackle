package io.emeraldpay.dshackle.auth.processor

import io.emeraldpay.dshackle.auth.AuthContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

class TokenProcessorTest {
    private val tokenProcessor = TokenProcessor()

    @BeforeEach
    fun removeSessions() {
        AuthContext.sessions.clear()
    }

    @Test
    fun `invalidate all tokens`() {
        AuthContext.putTokenInContext(
            AuthContext.TokenWrapper("token", Instant.now().minus(1, ChronoUnit.HOURS), "session1")
        )
        AuthContext.putTokenInContext(
            AuthContext.TokenWrapper("token", Instant.now().minus(1, ChronoUnit.HOURS), "session2")
        )
        AuthContext.putTokenInContext(
            AuthContext.TokenWrapper("token", Instant.now().minus(1, ChronoUnit.HOURS), "session3")
        )

        tokenProcessor.invalidateTokens()

        assertTrue(AuthContext.sessions.isEmpty())
    }

    @Test
    fun `tokens are still in the context after invalidation`() {
        val token1 = AuthContext.TokenWrapper("token", Instant.now().minus(30, ChronoUnit.MINUTES), "session1")
        val token2 = AuthContext.TokenWrapper("token", Instant.now().minus(30, ChronoUnit.MINUTES), "session2")
        val token3 = AuthContext.TokenWrapper("token", Instant.now().minus(30, ChronoUnit.MINUTES), "session3")
        AuthContext.putTokenInContext(token1)
        AuthContext.putTokenInContext(token2)
        AuthContext.putTokenInContext(token3)

        tokenProcessor.invalidateTokens()

        assertEquals(3, AuthContext.sessions.size)
        assertEquals(
            mapOf(token1.sessionId to token1, token2.sessionId to token2, token3.sessionId to token3),
            AuthContext.sessions
        )
    }
}
