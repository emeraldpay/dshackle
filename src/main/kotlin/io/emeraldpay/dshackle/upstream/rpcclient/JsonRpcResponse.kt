/**
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.rpcclient

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import reactor.core.publisher.Mono

class JsonRpcResponse(
    private val result: ByteArray?,
    val error: JsonRpcError?,
    val id: Id,
    val httpCode: Int? = null,
    /**
     * When making a request through Dshackle protocol a remote may provide its signature with the response, which we keep here
     */
    val providedSignature: ResponseSigner.Signature? = null,
) {
    constructor(result: ByteArray?, error: JsonRpcError?) : this(result, error, NumberId(0))

    companion object {
        private val NULL_VALUE = "null".toByteArray()

        @JvmStatic
        fun ok(value: ByteArray): JsonRpcResponse = JsonRpcResponse(value, null)

        @JvmStatic
        fun ok(
            value: ByteArray,
            id: Id,
        ): JsonRpcResponse = JsonRpcResponse(value, null, id)

        @JvmStatic
        fun ok(value: String): JsonRpcResponse = JsonRpcResponse(value.toByteArray(), null)

        @JvmStatic
        fun error(
            code: Int,
            msg: String,
        ): JsonRpcResponse = JsonRpcResponse(null, JsonRpcError(code, msg))

        @JvmStatic
        fun error(
            error: JsonRpcError,
            id: Id,
        ): JsonRpcResponse = JsonRpcResponse(null, error, id)

        @JvmStatic
        fun error(
            code: Int,
            msg: String,
            id: Id,
        ): JsonRpcResponse = JsonRpcResponse(null, JsonRpcError(code, msg), id)
    }

    fun hasResult(): Boolean = result != null

    fun hasError(): Boolean = error != null

    fun isNull(): Boolean = result != null && NULL_VALUE.contentEquals(result)

    val resultOrEmpty: ByteArray
        get() {
            return result ?: ByteArray(0)
        }

    val resultAsRawString: String
        get() {
            return String(resultOrEmpty)
        }

    val resultAsProcessedString: String
        get() {
            val str = resultAsRawString
            if (str.startsWith("\"") && str.endsWith("\"")) {
                return str.substring(1, str.length - 1)
            }
            throw IllegalStateException("Not as JS string")
        }

    fun requireResult(): Mono<ByteArray> =
        if (error != null) {
            Mono.error(error.asException(id))
        } else {
            Mono.just(resultOrEmpty)
        }

    fun requireStringResult(): Mono<String> =
        if (error != null) {
            Mono.error(error.asException(id))
        } else {
            Mono.just(resultAsProcessedString)
        }

    fun copyWithId(id: Id): JsonRpcResponse = JsonRpcResponse(result, error, id, httpCode, providedSignature)

    fun copyWithSignature(signature: ResponseSigner.Signature): JsonRpcResponse = JsonRpcResponse(result, error, id, httpCode, signature)

    fun copyWithHttpCode(httpCode: Int): JsonRpcResponse = JsonRpcResponse(result, error, id, httpCode, providedSignature)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is JsonRpcResponse) return false

        if (result != null) {
            if (other.result == null) return false
            if (!result.contentEquals(other.result)) return false
        } else if (other.result != null) {
            return false
        }
        if (error != other.error) return false

        return true
    }

    override fun hashCode(): Int {
        var result1 = result?.contentHashCode() ?: 0
        result1 = 31 * result1 + (error?.hashCode() ?: 0)
        return result1
    }

    /**
     * JSON RPC wrapper. Makes sure that the id is either Int or String
     */
    interface Id {
        fun asNumber(): Long

        fun asString(): String

        fun isNumber(): Boolean

        companion object {
            @JvmStatic
            fun from(id: Any): Id {
                if (id is Int) {
                    return NumberId(id)
                }
                if (id is Number) {
                    return NumberId(id.toLong())
                }
                if (id is String) {
                    return StringId(id)
                }
                throw IllegalArgumentException("Id must be Number or String")
            }
        }
    }

    class NumberId(
        val id: Long,
    ) : Id {
        constructor(id: Int) : this(id.toLong())

        override fun asNumber(): Long = id

        override fun asString(): String = throw IllegalStateException("Not string")

        override fun isNumber(): Boolean = true

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is NumberId) return false

            if (id != other.id) return false

            return true
        }

        override fun hashCode(): Int = id.hashCode()

        override fun toString(): String = id.toString()
    }

    class StringId(
        val id: String,
    ) : Id {
        override fun asNumber(): Long = throw IllegalStateException("Not a number")

        override fun asString(): String = id

        override fun isNumber(): Boolean = false

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is StringId) return false

            if (id != other.id) return false

            return true
        }

        override fun hashCode(): Int = id.hashCode()

        override fun toString(): String = id
    }

    class ResponseJsonSerializer : JsonSerializer<JsonRpcResponse>() {
        override fun serialize(
            value: JsonRpcResponse,
            gen: JsonGenerator,
            serializers: SerializerProvider,
        ) {
            gen.writeStartObject()
            gen.writeStringField("jsonrpc", "2.0")
            if (value.id.isNumber()) {
                gen.writeNumberField("id", value.id.asNumber())
            } else {
                gen.writeStringField("id", value.id.asString())
            }
            if (value.error != null) {
                gen.writeObjectFieldStart("error")
                gen.writeNumberField("code", value.error.code)
                gen.writeStringField("message", value.error.message)
                value.error.details?.let { details ->
                    when (details) {
                        is String -> gen.writeStringField("data", details)
                        is Number -> gen.writeNumberField("data", details.toInt())
                        else -> gen.writeObjectField("data", details)
                    }
                }
                gen.writeEndObject()
            } else {
                if (value.result == null) {
                    throw IllegalStateException("No result set")
                }
                gen.writeFieldName("result")
                gen.writeRaw(":")
                gen.writeRaw(String(value.result))
            }
            gen.writeEndObject()
        }
    }
}
