package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.Global

sealed interface CallParams {
    fun toJson(id: Int, method: String): ByteArray
}

abstract class JsonRpcParams : CallParams {

    override fun toJson(id: Int, method: String): ByteArray {
        val json = mapOf(
            "jsonrpc" to "2.0",
            "id" to id,
            "method" to method,
            "params" to params(),
        )

        return Global.objectMapper.writeValueAsBytes(json)
    }

    protected abstract fun params(): Any
}

data class ListParams(val list: List<Any>) : JsonRpcParams() {
    constructor(vararg elements: Any) : this(listOf(*elements))
    constructor() : this(listOf())

    override fun params(): Any {
        return list
    }
}

data class ObjectParams(val obj: Map<Any, Any>) : JsonRpcParams() {
    constructor(vararg pairs: Pair<Any, Any>) : this(mapOf(*pairs))

    override fun params(): Any {
        return obj
    }
}

data class RestParams(
    val headers: List<Pair<String, String>>,
    val queryParams: List<Pair<String, String>>,
    val pathParams: List<String>,
    val payload: ByteArray,
) : CallParams {
    companion object {
        fun emptyParams() = RestParams(emptyList(), emptyList(), emptyList(), ByteArray(0))
    }

    override fun toJson(id: Int, method: String): ByteArray {
        return payload
    }

    // we need equals and hashCode because of having ByteArray
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RestParams) return false

        if (headers != other.headers) return false
        if (queryParams != other.queryParams) return false
        if (pathParams != other.pathParams) return false
        if (!payload.contentEquals(other.payload)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = headers.hashCode()
        result = 31 * result + queryParams.hashCode()
        result = 31 * result + pathParams.hashCode()
        result = 31 * result + payload.contentHashCode()
        return result
    }
}
