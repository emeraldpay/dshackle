package io.emeraldpay.dshackle.upstream.stream

import io.emeraldpay.dshackle.upstream.ChainCallError
import reactor.core.publisher.Flux

sealed class Response

data class SingleResponse(
    val result: ByteArray?,
    val error: ChainCallError?,
) : Response() {
    fun hasError() = error != null

    fun noResponse() = result == null && error == null

    fun merge(other: SingleResponse?): SingleResponse {
        if (other == null) {
            return this
        }
        var newResult: ByteArray? = result
        var newError: ChainCallError? = error
        if (newResult == null && other.result != null) {
            newResult = other.result
        }
        if (newError == null && other.error != null) {
            newError = other.error
        }
        return SingleResponse(newResult, newError)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SingleResponse) return false

        if (result != null) {
            if (other.result == null) return false
            if (!result.contentEquals(other.result)) return false
        } else if (other.result != null) return false
        if (error != other.error) return false

        return true
    }

    override fun hashCode(): Int {
        var result1 = result?.contentHashCode() ?: 0
        result1 = 31 * result1 + (error?.hashCode() ?: 0)
        return result1
    }
}

data class StreamResponse(
    val stream: Flux<Chunk>,
) : Response()

data class AggregateResponse(
    val response: ByteArray,
    val code: Int,
) : Response() {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is AggregateResponse) return false

        if (!response.contentEquals(other.response)) return false
        if (code != other.code) return false

        return true
    }

    override fun hashCode(): Int {
        var result = response.contentHashCode()
        result = 31 * result + code
        return result
    }
}

data class Chunk(
    val chunkData: ByteArray,
    val finalChunk: Boolean,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Chunk) return false

        if (!chunkData.contentEquals(other.chunkData)) return false
        if (finalChunk != other.finalChunk) return false

        return true
    }

    override fun hashCode(): Int {
        var result = chunkData.contentHashCode()
        result = 31 * result + finalChunk.hashCode()
        return result
    }
}
