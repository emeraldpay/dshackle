package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.quorum.CallQuorum

class NoCallMethods : CallMethods {
    override fun createQuorumFor(method: String): CallQuorum = throw IllegalStateException("Method $method is not available")

    override fun isCallable(method: String): Boolean = false

    override fun getSupportedMethods(): Set<String> = emptySet()

    override fun isHardcoded(method: String): Boolean = false

    override fun executeHardcoded(method: String): ByteArray = throw IllegalStateException("Method $method is not available")
}
