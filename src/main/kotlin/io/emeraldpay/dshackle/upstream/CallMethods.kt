package io.emeraldpay.dshackle.upstream

interface CallMethods {
    fun getQuorumFor(method: String): CallQuorum
    fun isAllowed(method: String): Boolean
    fun getSupportedMethods(): Set<String>
    fun isHardcoded(method: String): Boolean
    fun hardcoded(method: String): Any
}