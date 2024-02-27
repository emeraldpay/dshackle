package io.emeraldpay.dshackle.upstream.rpcclient

sealed interface CallParams

data class ListParams(val list: List<Any>) : CallParams {
    constructor(vararg elements: Any) : this(listOf(*elements))
    constructor() : this(listOf())
}
data class ObjectParams(val obj: Map<Any, Any>) : CallParams {
    constructor(vararg pairs: Pair<Any, Any>) : this(mapOf(*pairs))
}
