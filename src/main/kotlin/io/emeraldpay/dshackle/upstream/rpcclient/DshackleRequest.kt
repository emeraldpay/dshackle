package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.upstream.Selector

data class DshackleRequest(
    val id: Int,
    val method: String,
    val params: List<Any?>,
    // internal properties
    val nonce: Long? = null,
    val matcher: Selector.Matcher = Selector.empty,
) {
    constructor(method: String, params: List<Any?>) : this(1, method, params)

    fun asJsonRequest(): JsonRpcRequest = JsonRpcRequest(method, params, id, nonce)
}
