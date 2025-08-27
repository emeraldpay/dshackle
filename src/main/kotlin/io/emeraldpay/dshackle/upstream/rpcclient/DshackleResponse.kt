package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import reactor.core.publisher.Mono

data class DshackleResponse(
    val id: Int,
    val result: ByteArray?,
    val error: JsonRpcError?,
    /**
     * When making a request through Dshackle protocol a remote may provide its signature with the response, which we keep here
     */
    val providedSignature: ResponseSigner.Signature? = null,
) {
    constructor(id: Int, result: ByteArray) : this(id, result, null)
    constructor(id: Int, error: JsonRpcError) : this(id, null, error)

    init {
        check(result == null || error == null)
    }

    val hasResult: Boolean
        get() = result != null

    val resultOrEmpty: ByteArray
        get() = result ?: ByteArray(0)

    val resultAsRawString: String
        get() = String(resultOrEmpty)

    val resultAsProcessedString: String
        get() {
            val str = resultAsRawString
            return if (str.startsWith("\"") && str.endsWith("\"")) {
                str.substring(1, str.length - 1)
            } else {
                throw IllegalStateException("Not as JS string")
            }
        }

    fun requireResult(): Mono<ByteArray> =
        if (error != null) {
            Mono.error(error.asException(JsonRpcResponse.NumberId(id)))
        } else {
            Mono.just(resultOrEmpty)
        }

    fun requireStringResult(): Mono<String> =
        if (error != null) {
            Mono.error(error.asException(JsonRpcResponse.NumberId(id)))
        } else {
            Mono.just(resultAsProcessedString)
        }
}
