package io.emeraldpay.dshackle.upstream

class DirectCallMethods : CallMethods {

    override fun getQuorumFor(method: String): CallQuorum {
        return AlwaysQuorum()
    }

    override fun isAllowed(method: String): Boolean {
        return true
    }

    override fun getSupportedMethods(): Set<String> {
        return emptySet()
    }

    override fun isHardcoded(method: String): Boolean {
        return false
    }

    override fun hardcoded(method: String): Any {
        return "unsupported"
    }
}